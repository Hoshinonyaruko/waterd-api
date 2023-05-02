import base64
import hashlib
from io import BytesIO
from typing import Optional
from PIL import Image
from fastapi import FastAPI, File, Form, Depends
from imagehash import phash
import aioredis

app = FastAPI()
PREFIX = "water"

def get_hamming_distance(hash1: int, hash2: int) -> int:
    xor_result = hash1 ^ hash2
    return bin(xor_result).count('1')

async def get_redis_pool():
    async with aioredis.create_redis_pool("redis://localhost", minsize=5, maxsize=20) as pool:
        yield pool

async def search_in_db(db, prefix, phash_value, group_id, user_id):
    async with db.pipeline() as pipe:
        pipe.keys(f"{prefix}:{phash_value}:*:{group_id}:*")
        pipe.keys(f"{prefix}:*:{phash_value}:{group_id}:*")
        keys1, keys2 = await pipe.execute()

    if keys1:
        key_parts = keys1[0].decode().split(":")
        return {
            "status": "duplicate",
            "group_id": key_parts[3],
            "user_id": key_parts[4],
            "timestamp": int(key_parts[5]),
        }

    min_hamming_distance = float("inf")
    closest_key = None
    for key in keys2:
        key_parts = key.decode().split(":")
        #hamming_distance = phash_value - phash(int(key_parts[2], 16))
        #hamming_distance = bin(int(key_parts[2], 16) ^ phash_value).count('1')
        hamming_distance = get_hamming_distance(int(key_parts[2], 16), phash_value)
        if hamming_distance < min_hamming_distance:
            min_hamming_distance = hamming_distance
            similarity_threshold = 5  # 自定义阈值
            print("min_hamming_distance",min_hamming_distance)
            if min_hamming_distance <= similarity_threshold:
                closest_key = key

    if closest_key:
        key_parts = closest_key.decode().split(":")
        return {
            "status": "similar",
            "hamming_distance": min_hamming_distance,
            "group_id": key_parts[3],
            "user_id": key_parts[4],
            "timestamp": int(key_parts[5]),
        }

    return {"status": "new"}

def create_key(prefix, md5_hash, phash_value, group_id, user_id, timestamp):
    return f"{prefix}:{md5_hash}:{phash_value}:{group_id}:{user_id}:{timestamp}"

@app.post("/upload/")
async def upload_image(
    image_base64: str = Form(...),
    group_id: str = Form(...),
    user_id: str = Form(...),
    timestamp: int = Form(...),
    db: aioredis.Redis = Depends(get_redis_pool),
):
    # 解码Base64图像数据并计算MD5值
    image_data = base64.b64decode(image_base64)
    md5_hash = hashlib.md5(image_data).hexdigest()

    # 还原图像数据并计算感知哈希值
    with BytesIO(image_data) as image_buffer:
        image = Image.open(image_buffer)
        phash_value = phash(image)

    # 构造键值
    key = create_key(PREFIX, md5_hash, phash_value, group_id, user_id, timestamp)
    await db.set(key, 1)

    # 在数据库中搜索
    result = await search_in_db(db, PREFIX, phash_value, group_id, user_id)
    return result

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=50117)