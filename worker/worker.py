from urllib import request
import redis
import os
from minio import Minio
import json
import time

print("Starting worker thread")

# Redis connection setup
redisHost = os.getenv("REDIS_HOST") or "redis.redis.svc.cluster.local"
redisPort = os.getenv("REDIS_PORT") or 6379

try:
    r = redis.StrictRedis(host=redisHost, port=redisPort, db=0)
    print(f"Connected to Redis at {redisHost}:{redisPort}")
except Exception as e:
    print(f"Error connecting to Redis: {str(e)}")
    exit(1)

# Queue keys
REDIS_KEY = "toWorkers"
RESULTS_QUEUE = "resultsQueue"
input_dir = "/tmp/input"
output_dir = "/tmp/output"

# Minio connection setup
DEFAULT_INPUT_BUCKET = "input-files"
minioHost = os.getenv("MINIO_HOST") or "minio-proj.minio.svc.cluster.local:9000"
minioUser = os.getenv("MINIO_USER") or "rootuser"
minioPasswd = os.getenv("MINIO_PASSWD") or "rootpass123"
print(f"Connecting to Minio at {minioHost}!")

try:
    MINIO_CLIENT = Minio(minioHost, access_key=minioUser, secret_key=minioPasswd, secure=False)
    print("Successfully connected to Minio")
except Exception as exp:
    print(f"Exception connecting to Minio: {str(exp)}")
    exit(1)

output_files = os.path.join(output_dir, "mdx_extra_q")

def create_bucket_if_not_exists(bucket_name):
    try:
        if not MINIO_CLIENT.bucket_exists(bucket_name):
            MINIO_CLIENT.make_bucket(bucket_name)
            print(f"Created bucket: {bucket_name}")
        else:
            print(f"Bucket {bucket_name} already exists.")
    except Exception as e:
        print(f"Error creating bucket {bucket_name}: {str(e)}")

def get_file_to_input_dir(file_name):
    try:
        create_bucket_if_not_exists(DEFAULT_INPUT_BUCKET)
        print(f"Downloading the file {file_name} from Minio...")
        file_path = os.path.join(input_dir, file_name)
        MINIO_CLIENT.fget_object(DEFAULT_INPUT_BUCKET, file_name, file_path)
        print(f"Placed file in temporary location: {file_path}")
        return file_path
    except Exception as e:
        print(f"Error downloading file {file_name} from Minio: {str(e)}")
        return None

def upload_file(bucket_name, file_name, file_path):
    try:
        create_bucket_if_not_exists(bucket_name)
        MINIO_CLIENT.fput_object(bucket_name, file_name, file_path)
        print(f"Uploaded file {file_name} to bucket {bucket_name}")
    except Exception as e:
        print(f"Error uploading file {file_name} to bucket {bucket_name}: {str(e)}")

def upload_dir(file_dir, bucket_name):
    try:
        files = os.listdir(file_dir)
        print(f"Uploading files from directory {file_dir} to bucket {bucket_name}: {files}")
        for file in files:
            file_path = os.path.join(file_dir, file)
            upload_file(bucket_name, file, file_path)
        print("Uploaded all files in directory.")
    except Exception as e:
        print(f"Error uploading files from directory {file_dir}: {str(e)}")

def delete_input_file(bucket_name, file_name):
    try:
        MINIO_CLIENT.remove_object(bucket_name, file_name)
        print(f"Deleted file {file_name} from bucket {bucket_name}")
    except Exception as e:
        print(f"Error deleting file {file_name} from bucket {bucket_name}: {str(e)}")

def user_counter():
    try:
        print("Waiting for a message in Redis queue...")
        message = r.blpop(REDIS_KEY)
        if message:
            print(f"Received message: {message}")
            file_data = json.loads(message[1].decode())
            file_name = file_data.get('file_name')
            context = file_data.get('context')

            if not file_name or not context:
                print("Invalid message format. Missing file_name or context.")
                return

            file_path = get_file_to_input_dir(file_name)
            if not file_path:
                print(f"Failed to download {file_name}. Skipping processing.")
                return

            command = f"python -u -m demucs.separate --out {output_dir} {file_path} --mp3"
            print(f"Executing command: {command}")
            os.system(command)

            result_bucket = file_name[:-4]
            create_bucket_if_not_exists(result_bucket)
            upload_dir(os.path.join(output_dir, "mdx_extra_q", result_bucket), result_bucket)

            # Send the result bucket name to the results queue
            r.rpush(RESULTS_QUEUE, json.dumps({"bucket_name": result_bucket}))
            print(f"Pushed result bucket {result_bucket} to {RESULTS_QUEUE}")

            # Delete input file after processing
            delete_input_file(DEFAULT_INPUT_BUCKET, file_name)
        else:
            print("No message found in the Redis queue.")
    except Exception as exp:
        print(f"Exception in worker loop: {str(exp)}")

while True:
    user_counter()
    time.sleep(1)

