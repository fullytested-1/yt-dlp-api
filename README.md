# yt-dlp API

Node.js + Express API using yt-dlp and FFmpeg.

## Endpoints
GET /health
GET /api/info?url=VIDEO_URL
GET /api/download?url=VIDEO_URL&format=mp4
GET /api/download?url=VIDEO_URL&format=mp3

## Koyeb
Create a Koyeb Web Service from this GitHub repository, select Dockerfile, expose port 8000, and deploy.

Optional:
MAX_FILE_BYTES=524288000
MAX_DOWNLOAD_AGE_MS=1800000

Use only for content you are authorized to download. Do not use it to bypass DRM, paywalls, authentication, or other access controls.