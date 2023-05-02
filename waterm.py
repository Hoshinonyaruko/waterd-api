import base64
import hashlib
from io import BytesIO
from typing import Optional
from PIL import Image
from fastapi import FastAPI, File, Form, Depends
from imagehash import phash
import aioredis
from datasketch import MinHash, MinHashLSH
import pickle


app = FastAPI()
PREFIX = "lsh"
THRESHOLD = 0.1
NUM_PERM = 128

lsh = MinHashLSH(threshold=THRESHOLD, num_perm=NUM_PERM)

async def load_lsh_index(redis_pool):
    async with redis_pool.client() as conn:
        keys = await conn.keys(f"{PREFIX}:minhash:*")
        for key in keys:
            minhash_data = await conn.get(key)
            minhash = pickle.loads(minhash_data)
            image_info = key.decode().replace(f"{PREFIX}:minhash:", "")
            lsh.insert(image_info, minhash)

@app.on_event("startup")
async def on_startup():
    redis_pool = await get_redis_pool()
    await load_lsh_index(redis_pool)


async def get_redis_pool():
    redis_pool = aioredis.Redis.from_url("redis://localhost")
    return redis_pool

async def search_in_db(conn, minhash, md5_hash, group_id, user_id, timestamp):
    # First check the MD5 value
    md5_key = await conn.get(f"{PREFIX}:md5:{md5_hash}")
    if md5_key:
        stored_md5_info = md5_key.decode()
        stored_group_id, stored_user_id, stored_timestamp = stored_md5_info.split(':')
        return {
            "status": "same",
            "group_id": stored_group_id,
            "user_id": stored_user_id,
            "timestamp": int(stored_timestamp),
        }

    # Use LSH query to find similar images
    #返回一个 符合相似度的minhash数组
    similar_images = lsh.query(minhash)

    # Store similarity scores in a list
    similarity_scores = []

    # Retrieve MinHash objects for each similar image and calculate Jaccard similarity
    for similar_image_key in similar_images:
        stored_minhash_data = await conn.get(f"{PREFIX}:minhash:{similar_image_key}")
        if stored_minhash_data:
            stored_minhash = pickle.loads(stored_minhash_data)
            similarity = minhash.jaccard(stored_minhash)
            similarity_scores.append((similar_image_key, similarity))
            print("发现相似图片:",similar_image_key,"相似度:",similarity)

    # Sort similarity scores in descending order
    similarity_scores.sort(key=lambda x: x[1], reverse=True)

    # Set similarity threshold (1/5 = 0.2)
    #其中 1 表示完全相似，0 表示完全不相似。
    similarity_threshold = 0.2

    # Check if the highest similarity score is above the threshold
    if similarity_scores and similarity_scores[0][1] >= similarity_threshold:
        highest_similarity_key = similarity_scores[0][0]
        stored_phash_value,stored_group_id, stored_user_id, stored_timestamp = highest_similarity_key.split(':')
        if group_id == stored_group_id:
            return {
                "status": "like",
                "group_id": stored_group_id,
                "user_id": stored_user_id,
                "timestamp": stored_timestamp,
                "phash_value": stored_phash_value,
            }

    return {"status": "new"}


def create_key(phash_value, group_id, user_id, timestamp):
    return f"{phash_value}:{group_id}:{user_id}:{timestamp}"

@app.post("/upload/")
async def upload_image(
    phash: str = Form(...),
    minhash_base64: str = Form(...),
    md5_hash: str = Form(...),
    group_id: str = Form(...),
    user_id: str = Form(...),
    timestamp: int = Form(...),
    db: aioredis.Redis = Depends(get_redis_pool),
):
    # Deserialize MinHash from base64
    minhash = pickle.loads(base64.b64decode(minhash_base64.encode()))

    # Search in the database
    result = await search_in_db(db, minhash, md5_hash, group_id, user_id, timestamp)
    
    if result["status"] == "new":
        # Add MinHash to LSH index
        key = create_key(phash, group_id, user_id, timestamp)
        if key not in lsh:
            lsh.insert(key, minhash)
        else:
            print(f"The key {key} already exists in the LSH index")

        # Serialize and store MinHash in Redis
        minhash_key = f"{PREFIX}:minhash:{key}"
        await db.set(minhash_key, pickle.dumps(minhash))
        md5_key = f"{PREFIX}:md5:{md5_hash}"
        await db.set(md5_key, f"{group_id}:{user_id}:{timestamp}")

    return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=50117)