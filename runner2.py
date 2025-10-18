import asyncio
import json
import time
import asyncpg
import aiohttp
import base64
import cv2
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_IN = "runner_tasks2"
TOPIC_OUT = "inference_frames"
runner_id = 2

DB_CONFIG = {
    "user": "postgres",
    "password": "mysecretpassword",
    "database": "postgres",
    "host": "localhost",
    "port": 5432
}

HEARTBEAT_INTERVAL = 3
API_URL = "http://localhost:5050/runner-status"


async def get_consumer():
    consumer = AIOKafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="runner-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )
    await consumer.start()
    return consumer


async def send_heartbeat():
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                await session.post(API_URL, json={
                    "runner_id": runner_id,
                    "timestamp": time.time()
                })
            except Exception as e:
                print(f"[Runner {runner_id}] Heartbeat failed: {e}")
            await asyncio.sleep(HEARTBEAT_INTERVAL)


async def process_scenario(data, conn, producer):
    if data.get("type") != "run_scenario":
        return

    scenario_id = data["scenario_id"]

    async with conn.acquire() as cur:
        row = await cur.fetchrow("SELECT status, vid_id FROM scenarios WHERE id = $1", scenario_id)
        if not row:
            print(f"[Runner {runner_id}] not found {scenario_id}")
            return

        if row["status"] != "in_startup_processing":
            print(f"[Runner {runner_id}] skip {scenario_id}")
            return

        vid_id = row["vid_id"]

        await cur.execute("""
            UPDATE scenarios
            SET status = 'active', runner_id = $1
            WHERE id = $2
        """, runner_id, scenario_id)

    print(f"[Runner {runner_id}] start {scenario_id} (vid_id={vid_id})")

    video_path = f"videos/{vid_id}.mp4"
    cap = cv2.VideoCapture(video_path)

    frame_index = 0
    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break

        if frame_index % 25 == 0:
            _, buffer = cv2.imencode('.jpg', frame)
            image_base64 = base64.b64encode(buffer).decode("utf-8")

            message = {
                "scenario_id": scenario_id,
                "frame_index": frame_index,
                "image_base64": image_base64
            }

            await producer.send_and_wait(TOPIC_OUT, message)
            #print(f"[Runner {runner_id}] sent {frame_index} of {scenario_id}")
            await asyncio.sleep(0.5)

        frame_index += 1

    cap.release()

    async with conn.acquire() as cur:
        await cur.execute("""
            UPDATE scenarios
            SET status = 'inactive'
            WHERE id = $1
        """, scenario_id)

    print(f"[Runner {runner_id}] ended {scenario_id}")


async def main():
    db_pool = await asyncpg.create_pool(**DB_CONFIG)
    consumer = await get_consumer()
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    await producer.start()
    asyncio.create_task(send_heartbeat())

    print(f"[Runner {runner_id}] started")

    try:
        async for msg in consumer:
            data = msg.value
            asyncio.create_task(process_scenario(data, db_pool, producer))
    finally:
        await consumer.stop()
        await producer.stop()
        await db_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
