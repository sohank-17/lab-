from urllib import request
import redis
import os
from minio import Minio
import json
import time

print("Starting worker thread")

# Redis connection setup
redisHost = os.getenv("REDIS_HOST") or "localhost"
redisPort = os.getenv("REDIS_PORT") or 6379

try:
    r = redis.StrictRedis(host=redisHost, port=redisPort, db=0)
    print(f"Connected to Redis at {redisHost}:{redisPort}")
except Exception as e:
    print(f"Error connecting to Redis: {str(e)}")
    exit(1)

REDIS_KEY = "toWorkers"
RESULTS_QUEUE = "resultsQueue"  # New Redis queue for results
input_dir = "/tmp/input"
output_dir = "/tmp/output"

# Minio connection setup
BUCKET_NAME = "working"
minioHost = os.getenv("MINIO_HOST") or "minio-proj.minio.svc.cluster.local:9000"
minioUser = os.getenv("MINIO_USER") or "rootuser"
minioPasswd = os.getenv("MINIO_PASSWD") or "rootpass123"
print(f"Getting Minio connection now for host {minioHost}!")

MINIO_CLIENT = None
try:
    MINIO_CLIENT = Minio(minioHost, access_key=minioUser, secret_key=minioPasswd, secure=False)
    print("Got Minio connection", MINIO_CLIENT)
except Exception as exp:
    print(f"Exception raised in worker loop: {str(exp)}")
    exit(1)

output_files = os.path.join(output_dir, "mdx_extra_q")

def get_file_to_input_dir(file_name):
    try:
        print(f"Downloading the file {file_name} from Minio...")
        file_path = os.path.join(input_dir, file_name)
        MINIO_CLIENT.fget_object(BUCKET_NAME, file_name, file_path)
        print(f"Placed file in temporary location: {file_path}")
        print(f"Files in input directory: {os.listdir(input_dir)}")
        return file_path
    except Exception as e:
        print(f"Error downloading file {file_name} from Minio: {str(e)}")
        return None

def create_bucket(bucket_name):
    try:
        found = MINIO_CLIENT.bucket_exists(bucket_name)
        if not found:
            MINIO_CLIENT.make_bucket(bucket_name)
            print(f"Created new bucket: {bucket_name}")
        else:
            print(f"Bucket {bucket_name} already exists.")
    except Exception as e:
        print(f"Error creating bucket {bucket_name}: {str(e)}")

def upload_file(file_name, bucket_name, file_path):
    try:
        MINIO_CLIENT.fput_object(bucket_name, file_name, file_path)
        print(f"Uploaded file {file_name} to bucket {bucket_name}")
    except Exception as e:
        print(f"Error uploading file {file_name} to bucket {bucket_name}: {str(e)}")

def upload_dir(file_dir, file_hash):
    try:
        files = os.listdir(file_dir)
        print(f"Available files in directory {file_dir}: {files}")
        for file in files:
            print(f"Uploading file {file} to bucket {file_hash}")
            file_path = os.path.join(file_dir, file)
            upload_file(file, file_hash, file_path)
        print("Uploaded all files in directory.")
    except Exception as e:
        print(f"Error uploading files from directory {file_dir}: {str(e)}")

def push_results_to_results_queue(file_name, bucket_name):
    try:
        # Create the result metadata
        result_data = {
            "file_name": file_name,
            "bucket_name": bucket_name,
            "status": "completed",  # You can add more status information if needed
            "message": "File successfully separated and uploaded."
        }
        # Push the result metadata into the results queue
        r.rpush(RESULTS_QUEUE, json.dumps(result_data))
        print(f"Pushed result to {RESULTS_QUEUE}: {result_data}")
    except Exception as e:
        print(f"Error pushing result to Redis: {str(e)}")

def user_counter():
    try:
        print("Looking for a message in Redis queue...")
        message = r.blpop(REDIS_KEY)  # Watch the 'toWorkers' queue for tasks
        if message:
            print(f"Found message: {message}")
            file_data = json.loads(message[1].decode())
            print(f"Parsed file data: {file_data}")

            file_name = file_data.get('file_name')
            context = file_data.get('context')
            print(f"File name: {file_name}, Context: {context}")

            if not file_name or not context:
                print(f"Invalid message format. Missing file_name or context.")
                return

            file_path = get_file_to_input_dir(file_name)
            if file_path is None:
                print(f"Failed to download file {file_name}. Skipping processing.")
                return

            command = f"python -u -m demucs.separate --out {output_dir} {file_path} --mp3"
            print(f"Executing command: {command}")
            os.system(command)

            bucket_name = file_name[:-4]  # Use file name without extension as bucket name
            create_bucket(bucket_name)
            upload_dir(os.path.join(output_dir, "mdx_extra_q", bucket_name), bucket_name)

            # After processing, push the result into the RESULTS_QUEUE
            push_results_to_results_queue(file_name, bucket_name)

        else:
            print("No message found in the Redis queue.")
    except Exception as exp:
        print(f"Exception raised in worker loop: {str(exp)}")

while True:
    user_counter()
    time.sleep(1)  # Sleep for 1 second before looking for the next message

