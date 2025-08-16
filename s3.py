import boto3
from botocore.exceptions import NoCredentialsError
import os
from urllib.parse import urlparse

def init_s3_client():
    """
    Initialize a global S3 client instance synchronously.
    """
    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION","ap-south-1")
        )
        print("S3 client initialized successfully!")
        return s3_client
    except NoCredentialsError:
        print("Invalid AWS credentials provided")



def download_video_from_s3(video_url: str, video_id: str, temp_dir: str, s3_client):
    """
    Download a video from S3 using the global s3_client and save it locally.
    Args:
        video_url: The full S3 key/path to the video (e.g. 'long_video/abc123.mp4')
        video_id: The unique ID of the video (for local filename)
        temp_dir: Local directory to save the file
        s3_client: Initialized S3 client
    """
    bucket_name = os.getenv("AWS_S3_BUCKET")

    # Ensure temp_dir exists
    os.makedirs(temp_dir, exist_ok=True)

    # Local file path - using video_id for filename but keeping original extension
    file_ext = os.path.splitext(video_url)[1] or '.mp4'
    input_file_name = f"{video_id}{file_ext}"
    input_file_path = os.path.join(temp_dir, input_file_name)
    
    try:
        # Verify the file exists first
        s3_client.head_object(Bucket=bucket_name, Key=video_url)
        
        # Download the file
        s3_client.download_file(bucket_name, video_url, input_file_path)
        print(f"Successfully downloaded to {input_file_path}")
        return input_file_path, input_file_name
        
    except Exception as e:
        raise RuntimeError(f"Failed to download video from S3 (Bucket: {bucket_name}, Key: {video_url}): {e}")