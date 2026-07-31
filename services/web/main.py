import os

import httpx
from config import require, seal
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

UPLOAD_API_URL = require("UPLOAD_API_URL")
RTMP_PUBLIC_URL = require("RTMP_PUBLIC_URL")
# AWS: the CloudFront domain, HLS keys start at "hls/".
# Compose: MinIO, where "/media" is the bucket segment of a path-style URL.
HLS_PUBLIC_BASE_URL = os.environ.get("HLS_PUBLIC_BASE_URL") or (
    require("MINIO_PUBLIC_URL") + "/media"
)
seal()


@app.get("/health")
def health():
    return {"status": "ok"}


# Alias for the k8s liveness/startup/readiness probes (see
# docs/aws/11-workloads.md) -- readiness is deliberately /livez too, not a
# check on upload-api: web calls it on every page render, and making web
# NotReady when upload-api is down would turn one outage into two.
@app.get("/livez")
def livez():
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    with httpx.Client(base_url=UPLOAD_API_URL) as client:
        streams = client.get("/streams").json()
        videos = client.get("/videos").json()
    # The live-stream list changes as soon as someone goes on/off air —
    # a cached copy of this page can send a viewer to a "Watch" link for a
    # stream that already ended (or hide one that just went live).
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "streams": streams, "videos": videos},
        headers={"Cache-Control": "no-store"},
    )


@app.get("/streamers/new", response_class=HTMLResponse)
def new_streamer_form(request: Request):
    return templates.TemplateResponse("streamer_new.html", {"request": request, "result": None})


@app.post("/streamers/new", response_class=HTMLResponse)
def new_streamer_submit(request: Request, display_name: str = Form(...)):
    with httpx.Client(base_url=UPLOAD_API_URL) as client:
        result = client.post("/streamers", json={"display_name": display_name}).json()
    rtmp_url = f"{RTMP_PUBLIC_URL}/{result['stream_key']}"
    return templates.TemplateResponse(
        "streamer_new.html", {"request": request, "result": result, "rtmp_url": rtmp_url}
    )


@app.get("/upload", response_class=HTMLResponse)
def upload_form(request: Request):
    return templates.TemplateResponse("upload.html", {"request": request, "result": None})


@app.post("/upload", response_class=HTMLResponse)
async def upload_submit(request: Request):
    form = await request.form()
    file = form["file"]
    with httpx.Client(base_url=UPLOAD_API_URL, timeout=60) as client:
        result = client.post(
            "/videos",
            data={"stream_key": form["stream_key"], "title": form["title"]},
            files={"file": (file.filename, await file.read(), file.content_type)},
        ).json()
    return templates.TemplateResponse("upload.html", {"request": request, "result": result})


# Records a view (real pipeline: web -> upload-api -> Kafka -> analytics-worker
# -> Postgres) and plays the real HLS output with hls.js. content_id is
# always the streams/videos row UUID (never the stream's secret path/key —
# see research_validated_architecture memory), so it's always safe to put in
# a URL the browser sees, for both content types.
@app.get("/watch/{content_type}/{content_id}", response_class=HTMLResponse)
def watch(request: Request, content_type: str, content_id: str):
    with httpx.Client(base_url=UPLOAD_API_URL) as client:
        client.post("/analytics/view", json={"content_type": content_type, "content_id": content_id})
        # /streams only lists status='live' rows and /videos has no
        # per-id lookup, so a stream that already ended (or, in principle,
        # a video row that's since vanished) just falls back to a generic
        # heading below rather than a failed request.
        listing = client.get("/streams" if content_type == "stream" else "/videos").json()
    match = next((item for item in listing if str(item["id"]) == content_id), None)
    if content_type == "stream":
        heading = match["display_name"] if match else "Live stream"
        subtitle = None
    else:
        heading = match["title"] if match else "Video"
        subtitle = f"by {match['display_name']}" if match else None

    prefix = "live" if content_type == "stream" else "vod"
    manifest_url = f"{HLS_PUBLIC_BASE_URL}/hls/{prefix}/{content_id}/master.m3u8"
    return templates.TemplateResponse(
        "watch.html",
        {
            "request": request,
            "content_type": content_type,
            "content_id": content_id,
            "manifest_url": manifest_url,
            "heading": heading,
            "subtitle": subtitle,
        },
        # A cached page response means the browser skips the request
        # entirely on a revisit — silently dropping the /analytics/view
        # call above, not just risking stale content.
        headers={"Cache-Control": "no-store"},
    )
