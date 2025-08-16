import os
import shutil
import tempfile
import subprocess
import requests
from PIL import Image
import imagehash
from s3 import download_video_from_s3
def save_file_buffer(file_buffer, file_mime_type, video_id, temp_dir):
    """Save file buffer to a temp directory."""
    try:
        ext = file_mime_type.split('/')[1]
        input_file_name = f"temp-{video_id}.{ext}"
        input_file_path = os.path.join(temp_dir, input_file_name)
        with open(input_file_path, 'wb') as f:
            f.write(file_buffer)
        return input_file_path, input_file_name
    except Exception as e:
        raise RuntimeError(f"Error saving file: {e}")



def generate_phash(file_paths):
    """Generate perceptual hashes for a list of images."""
    hashes = []
    for file_path in file_paths:
        img = Image.open(file_path)
        phash = imagehash.phash(img, hash_size=16)  # Similar to Node version
        hashes.append(phash.__str__())
    return hashes


def hex_to_binary(hex_str):
    return ''.join(bin(int(h, 16))[2:].zfill(4) for h in hex_str)


def binary_to_hex(bin_str):
    return ''.join(f"{int(bin_str[i:i+4], 2):x}" for i in range(0, len(bin_str), 4))


def bitwise_avg_hashes(p_hashes):
    binary_hashes = [hex_to_binary(h) for h in p_hashes]
    bit_length = len(binary_hashes[0])
    bit_sums = [0] * bit_length

    for bh in binary_hashes:
        for i in range(bit_length):
            bit_sums[i] += 1 if bh[i] == '1' else 0

    threshold = len(p_hashes) / 2
    final_binary = ''.join('1' if s > threshold else '0' for s in bit_sums)
    return binary_to_hex(final_binary)


def get_frames(file_path, output_dir, file_name):
    """Extract frames using ffmpeg."""
    os.makedirs(output_dir, exist_ok=True)
    output_pattern = os.path.join(output_dir, f"{file_name}_%04d.jpg")
    cmd = [
        "ffmpeg", "-i", file_path, "-vf", "fps=1", "-frames:v", "100", output_pattern
    ]
    subprocess.run(cmd, check=True)

    return [
        os.path.join(output_dir, f)
        for f in os.listdir(output_dir)
        if f.endswith(".jpg")
    ]


def cleanup(*dirs):
    """Delete temporary directories."""
    for d in dirs:
        if os.path.exists(d):
            shutil.rmtree(d)


def create_dirs(*dirs):
    """Create directories if they don't exist."""
    for d in dirs:
        os.makedirs(d, exist_ok=True)


def fingerprint_video(video_id, video_url,s3_client):
    temp_dir = tempfile.mkdtemp()
    output_dir = tempfile.mkdtemp()

    try:
        input_file_path, input_file_name = download_video_from_s3(video_url,video_id,temp_dir,s3_client)

        # Extract frames
        files = get_frames(input_file_path, output_dir, input_file_name)

        # Generate hashes
        p_hashes = generate_phash(files)

        # Average hash
        fingerprint = bitwise_avg_hashes(p_hashes)

        return fingerprint

    finally:
        cleanup(temp_dir, output_dir)


def compare_hamming_distance(str1, str2, threshold=5):
    """Compare hashes with Hamming distance."""
    bin1 = hex_to_binary(str1)
    bin2 = hex_to_binary(str2)

    if len(bin1) != len(bin2):
        return False

    dif = sum(1 for a, b in zip(bin1, bin2) if a != b)
    return dif < threshold
