from flask import Flask, request, send_file, jsonify
import redis
import os
import json
from minio import Minio
from io import BytesIO

app = Flask(__name__)

# Redis connection setup
redisHost = os.getenv("REDIS_HOST") or "redis.redis.svc.cluster.local"
redisPort = os.getenv("REDIS_PORT") or 6379

try:
    r = redis.StrictRedis(host=redisHost, port=redisPort, db=0)
    print(f"Connected to Redis at {redisHost}:{redisPort}")
except Exception as e:
    print(f"Error connecting to Redis: {str(e)}")
    exit(1)

# Minio connection setup
DEFAULT_INPUT_BUCKET = "input-files"
minioHost = os.getenv("MINIO_HOST") or "minio-proj.minio.svc.cluster.local:9000"
minioUser = os.getenv("MINIO_USER") or "rootuser"
minioPasswd = os.getenv("MINIO_PASSWD") or "rootpass123"

try:
    MINIO_CLIENT = Minio(minioHost, access_key=minioUser, secret_key=minioPasswd, secure=False)
    print("Connected to Minio")
except Exception as exp:
    print(f"Error connecting to Minio: {str(exp)}")
    exit(1)

RESULTS_QUEUE = "resultsQueue"

def create_bucket_if_not_exists(bucket_name):
    try:
        if not MINIO_CLIENT.bucket_exists(bucket_name):
            MINIO_CLIENT.make_bucket(bucket_name)
            print(f"Created bucket: {bucket_name}")
        else:
            print(f"Bucket {bucket_name} already exists.")
    except Exception as e:
        print(f"Error creating bucket {bucket_name}: {str(e)}")

@app.route('/upload', methods=['POST'])
def upload():
    if 'file' not in request.files:
        return "No file provided", 400

    file = request.files['file']
    filename = file.filename

    # Create input bucket if not exists
    create_bucket_if_not_exists(DEFAULT_INPUT_BUCKET)

    temp_path = f"/tmp/{filename}"
    file.save(temp_path)
    MINIO_CLIENT.fput_object(DEFAULT_INPUT_BUCKET, filename, temp_path)

    # Push a message to the worker queue
    r.rpush("toWorkers", json.dumps({"file_name": filename, "context": "separation"}))
    print(f"Uploaded {filename} to Minio and pushed to Redis queue.")

    return jsonify({"message": "File uploaded and processing started"}), 200

@app.route('/results', methods=['GET'])
def get_results():
    result = r.blpop(RESULTS_QUEUE, timeout=30)  # Wait up to 30 seconds for a result
    if not result:
        return jsonify({"message": "No results available yet"}), 404

    result_data = json.loads(result[1].decode())
    bucket_name = result_data.get('bucket_name')

    if not bucket_name:
        return jsonify({"error": "Invalid result data"}), 500

    files = MINIO_CLIENT.list_objects(bucket_name)
    files_info = []
    for file in files:
        data = MINIO_CLIENT.get_object(bucket_name, file.object_name)
        temp_file_path = f"/tmp/{file.object_name}"
        with open(temp_file_path, 'wb') as f:
            f.write(data.read())
        files_info.append({
            "file_name": file.object_name,
            "download_link": f"/download/{bucket_name}/{file.object_name}"
        })

    # Delete the result bucket after fetching
    for file in files_info:
        MINIO_CLIENT.remove_object(bucket_name, file['file_name'])
    MINIO_CLIENT.remove_bucket(bucket_name)
    print(f"Deleted result bucket {bucket_name} after fetching files.")

    return jsonify({"files": files_info}), 200

@app.route('/download/<bucket_name>/<filename>', methods=['GET'])
def download_file(bucket_name, filename):
    try:
        data = MINIO_CLIENT.get_object(bucket_name, filename)
        return send_file(BytesIO(data.read()), download_name=filename, as_attachment=True)
    except Exception as e:
        print(f"Error downloading file {filename} from bucket {bucket_name}: {str(e)}")
        return jsonify({"error": f"File {filename} not found or error occurred"}), 500
