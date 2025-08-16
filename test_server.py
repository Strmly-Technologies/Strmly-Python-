from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
from nsfw import detect_nsfw_video
from video_fingerprint import fingerprint_video,compare_hamming_distance
from audio_fingerprint import fingerprint_audio

app = FastAPI()


class VideoRequest(BaseModel):
    videoUrl: str
    videoId: str

 


@app.get("/health-check")
async def healthCheck():
    return {"message": "ok"}


@app.post("/api/v1/detect-nsfw")
async def detect_nsfw_endpoint(request: VideoRequest):
    try:
        is_nsfw=detect_nsfw_video(request.videoUrl,request.videoId)
 
        return {
            "videoId": request.videoId,
            "videoUrl":request.videoUrl,
            "is_nsfw": is_nsfw
        }

    except HTTPException as e:
        raise e
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str("Internal Server Error"))



@app.post("/api/v1/fingerprint-video")
async def generate_video_fingerprint_endpoint(request: VideoRequest):
    try:
        fingerprint = fingerprint_video(request.videoId, request.videoUrl)
        fingerprint_hex = fingerprint.hex() if isinstance(fingerprint, bytes) else fingerprint
        is_duplicate=compare_hamming_distance("df88e6d0cc9a1a1c1cc9c0bf4e3e8a8b4f53b61826320eb5cb32d2e2eebc9a74",fingerprint_hex)
        return {
            "videoId": request.videoId,
            "videoUrl": request.videoUrl,
            "video_fingerprint": fingerprint_hex
        }

    except HTTPException as e:
        raise e
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str("Internal Server Error"))


@app.post("/api/v1/fingerprint-audio")
async def fingerprint_audio_endpoint(request: VideoRequest):
    try:
        fingerprint=fingerprint_audio(request.videoUrl,request.videoId)
        fingerprint_hex = fingerprint.hex() if isinstance(fingerprint, bytes) else fingerprint
        is_duplicate=compare_hamming_distance("df88e6d0cc9a1a1c1cc9c0bf4e3e8a8b4f53b61826320eb5cb32d2e2eebc9a74",fingerprint_hex)
        return {
            "videoId": request.videoId,
            "videoUrl": request.videoUrl,
            "audio_fingerprint": fingerprint_hex,
            "is_duplicate":is_duplicate
        }

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str("Internal Server Error"))