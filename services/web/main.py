import os

import httpx
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

app = FastAPI()
templates = Jinja2Templates(directory="templates")
UPLOAD_API_URL = os.environ["UPLOAD_API_URL"]
MINIO_PUBLIC_URL = os.environ["MINIO_PUBLIC_URL"]
RTMP_PUBLIC_URL = os.environ["RTMP_PUBLIC_URL"]


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    with httpx.Client(base_url=UPLOAD_API_URL) as client:
        streams = client.get("/streams").json()
        videos = client.get("/videos").json()
    return templates.TemplateResponse(
        "index.html", {"request": request, "streams": streams, "videos": videos}
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
    prefix = "live" if content_type == "stream" else "vod"
    manifest_url = f"{MINIO_PUBLIC_URL}/media/hls/{prefix}/{content_id}/master.m3u8"
    return templates.TemplateResponse(
        "watch.html",
        {
            "request": request,
            "content_type": content_type,
            "content_id": content_id,
            "manifest_url": manifest_url,
        },
    )
