import os
import subprocess
import tempfile
import requests
import numpy as np
import librosa
import hashlib
from s3 import download_video_from_s3
from video_fingerprint import cleanup
def download_video(video_url: str) -> str:
    """
    Downloads a video from the given URL and saves it to a temporary file.
    Returns the path to the saved video.
    """
    temp_dir = tempfile.mkdtemp()
    video_path = os.path.join(temp_dir, "video.mp4")

    response = requests.get(video_url, stream=True)
    response.raise_for_status()

    with open(video_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    return video_path


def extract_audio(video_path: str) -> str:
    """
    Extracts audio from a video file using ffmpeg and saves it as a temporary WAV file.
    """
    audio_path = video_path.replace(".mp4", ".wav")

    cmd = [
        "ffmpeg", "-i", video_path,
        "-vn",  # no video
        "-acodec", "pcm_s16le",  # uncompressed WAV
        "-ar", "44100",  # sample rate
        "-ac", "2",  # stereo
        audio_path,
        "-y"  # overwrite if exists
    ]
    subprocess.run(cmd, check=True)

    return audio_path


def generate_audio_fingerprint(audio_path: str) -> str:
    """
    Generates an audio fingerprint using MFCC features and SHA256 hashing.
    """
    y, sr = librosa.load(audio_path, sr=None)
    mfcc = librosa.feature.mfcc(y=y, sr=sr, n_mfcc=20)
    mfcc_mean = np.mean(mfcc, axis=1)  # average across time

    # Convert MFCC array to bytes and hash it
    mfcc_bytes = mfcc_mean.tobytes()
    fingerprint_hash = hashlib.sha256(mfcc_bytes).hexdigest()

    return fingerprint_hash



def fingerprint_audio(video_url, video_id,s3_client):
    temp_dir = tempfile.mkdtemp()
    output_dir = tempfile.mkdtemp()
    try:
        # Step 1: Download video
        input_file_path, input_file_name = download_video_from_s3(video_url,video_id,temp_dir,s3_client)

        # Step 2: Extract audio
        audio_path = extract_audio(input_file_path)

        # Step 3: Generate audio fingerprint
        fingerprint = generate_audio_fingerprint(audio_path)

        return fingerprint
    finally:
        cleanup(temp_dir,output_dir)