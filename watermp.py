import base64
import hashlib
from io import BytesIO
from PIL import Image
from fastapi import FastAPI, Form
from imagehash import phash
from datasketch import MinHash
import pickle
import requests
import time

app = FastAPI()
THRESHOLD = 0.8
NUM_PERM = 128

def phash_to_minhash(phash_value):
    phash_str = str(phash_value)
    minhash = MinHash(num_perm=NUM_PERM)
    for i in range(0, len(phash_str), 4):
        minhash.update(phash_str[i:i+4].encode('utf-8'))
    return minhash

@app.post("/process_image/")
async def process_image(
    image_url: str = Form(...),
    group_id: str = Form(...),
    user_id: str = Form(...)
):
    # Download image data from the URL
    response = requests.get(image_url)
    response.raise_for_status()
    image_data = response.content

    # Calculate MD5 value
    md5_hash = hashlib.md5(image_data).hexdigest()

    # Restore image data and calculate perceptual hash value
    with BytesIO(image_data) as image_buffer:
        image = Image.open(image_buffer)
        hash_size = 16
        highfreq_factor = 8
        phash_value = phash(image, hash_size, highfreq_factor)

    # Compute MinHash for the image
    minhash = phash_to_minhash(phash_value)

    # Encode MinHash as base64
    minhash_base64 = base64.b64encode(pickle.dumps(minhash)).decode()

    # Get the current timestamp
    timestamp = timestamp = int(time.time())

    return {
        "phash": str(phash_value),
        "minhash_base64": minhash_base64,
        "md5_hash": md5_hash,
        "group_id": group_id,
        "user_id": user_id,
        "timestamp": timestamp,
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=50116)