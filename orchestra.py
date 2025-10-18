import asyncio
import json
import time
import asyncpg
from aiohttp import web
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_OUT = "runner_tasks2"
TOPIC_IN = "new_scenarios"
HEARTBEAT_TIMEOUT = 10

runner_heartbeats = {}
db_pool = None


async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(
        user="postgres",
        password="mysecretpassword",
        database="postgres",
        host="localhost",
        port=5432
    )


async def runner_status(request):
    data = await request.json()
    runner_id = data.get("runner_id")
    ts = data.get("timestamp", time.time())
    if runner_id:
        runner_heartbeats[runner_id] = ts
        return web.json_response({"status": "received"})
    return web.json_response({"error": "no runner id"}, status=400)

async def run_web_server():
    app = web.Application()
    app.router.add_post("/runner-status", runner_status)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, port=5050).start()

async def consume_new_scenarios(producer: AIOKafkaProducer):
    consumer = AIOKafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="orchestrator-group",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )
    await consumer.start()
    print("[Orchestrator] kafka consumer started.")

    try:
        async for msg in consumer:
            data = msg.value
            if data.get("type") == "new_scenario":
                scenario_id = data.get("scenario_id")
                print(f"[Orchestrator] new scenario: {scenario_id}")

                async with db_pool.acquire() as conn:
                    await conn.execute("""
                        UPDATE scenarios
                        SET status = 'in_startup_processing', runner_id = NULL
                        WHERE id = $1
                    """, scenario_id)

                # Отправляем в Kafka
                await producer.send_and_wait(TOPIC_OUT, {
                    "type": "run_scenario",
                    "scenario_id": str(scenario_id)
                })

                print(f"[Orchestrator] sent {scenario_id}")
    finally:
        await consumer.stop()

async def check_runner_health_loop(producer):
    while True:
        try:
            await check_runner_health(producer)
        except Exception as e:
            print(f"[HealthCheck] err: {e}")
        await asyncio.sleep(3)

async def check_runner_health(producer: AIOKafkaProducer):
    now = time.time()
    recently_dispatched = set()

    async with db_pool.acquire() as conn:
        for rid, last in list(runner_heartbeats.items()):
            if now - last > HEARTBEAT_TIMEOUT:
                print(f"[Orchestrator] runner {rid} no heartbeat")

                rows = await conn.fetch("""
                    SELECT id FROM scenarios
                    WHERE status = 'active' AND runner_id = $1
                """, rid)

                for row in rows:
                    sid = row["id"]
                    recently_dispatched.add(sid)

                    await conn.execute("""
                        UPDATE scenarios
                        SET status = 'in_startup_processing', runner_id = NULL
                        WHERE id = $1
                    """, sid)

                    await producer.send_and_wait(TOPIC_OUT, {
                        "type": "run_scenario",
                        "scenario_id": str(sid)
                    })
                    print(f"[Orchestrator] resent {sid}")

                del runner_heartbeats[rid]

        rows = await conn.fetch("""
            SELECT id FROM scenarios
            WHERE status = 'in_startup_processing' AND runner_id IS NULL
        """)
        for row in rows:
            sid = row["id"]
            if sid in recently_dispatched:
                continue
            try:
                await producer.send_and_wait(TOPIC_OUT, {
                    "type": "run_scenario",
                    "scenario_id": str(sid)
                })
                print(f"[Orchestrator] resent  {sid}")
            except Exception as e:
                print(f"[Orchestrator] err {sid}: {e}")

async def main():
    await init_db()
    await run_web_server()

    async with AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    ) as producer:
        print("[Orchestrator] started")
        await asyncio.gather(
            consume_new_scenarios(producer),
            check_runner_health_loop(producer)
        )

if __name__ == "__main__":
    asyncio.run(main())
