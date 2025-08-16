import os
import cv2
import tempfile
import requests
import torch
from PIL import Image
from transformers import AutoModelForImageClassification, ViTImageProcessor
from fastapi import HTTPException
from s3 import download_video_from_s3
from video_fingerprint import cleanup
# Load NSFW model once
model = AutoModelForImageClassification.from_pretrained("Falconsai/nsfw_image_detection")
processor = ViTImageProcessor.from_pretrained("Falconsai/nsfw_image_detection")
model.eval()

def download_video(url: str) -> str:
    """Download a video from a URL to a temporary file."""
    response = requests.get(url, stream=True)
    if response.status_code != 200:
        raise HTTPException(status_code=400, detail="Failed to download video.")
    
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
    with open(temp_file.name, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    return temp_file.name

def extract_frames(video_path: str):
    """Extract frames every 10 seconds from a video."""
    frames = []
    cap = cv2.VideoCapture(video_path)

    fps = cap.get(cv2.CAP_PROP_FPS)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    duration_sec = int(total_frames / fps) if fps > 0 else 0

    for sec in range(0, duration_sec, 10):
        cap.set(cv2.CAP_PROP_POS_MSEC, sec * 1000)
        success, frame = cap.read()
        if success:
            rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            img = Image.fromarray(rgb)
            frames.append(img)
    cap.release()
    return frames

def nsfw_detection(frames):
    """Run NSFW detection on a list of PIL images."""
    with torch.no_grad():
        for img in frames:
            inputs = processor(images=img, return_tensors="pt")
            outputs = model(**inputs)
            logits = outputs.logits
            predicted_label = logits.argmax(-1).item()
            label = model.config.id2label[predicted_label]
            if label=="nsfw":
                return True
    return False



def detect_nsfw_video(video_url,video_id,s3_client):
 temp_dir = tempfile.mkdtemp()
 output_dir = tempfile.mkdtemp()
 try:
    input_file_path, input_file_name = download_video_from_s3(video_url,video_id,temp_dir,s3_client)
    frames=extract_frames(input_file_path)
    is_nsfw=nsfw_detection(frames)
    return is_nsfw

 finally:
    cleanup(temp_dir,output_dir)


 