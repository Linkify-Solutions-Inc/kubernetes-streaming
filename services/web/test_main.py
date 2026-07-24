import os

os.environ.setdefault("UPLOAD_API_URL", "http://upload-api.test")
os.environ.setdefault("MINIO_PUBLIC_URL", "http://minio.test")
os.environ.setdefault("RTMP_PUBLIC_URL", "rtmp://rtmp.test")

import main  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

# main.py mounts StaticFiles(directory="static") and Jinja2Templates(directory="templates")
# at import time, both relative paths -- this test must run with services/web
# as the working directory (see the CI workflow's working-directory setting).
client = TestClient(main.app)


def test_health():
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}
