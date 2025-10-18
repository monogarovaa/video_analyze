import uuid
import json
import asyncpg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer
import asyncio
import uvicorn

app = FastAPI()

DB_CONFIG = {
    'user': 'postgres',
    'password': 'mysecretpassword',
    'database': 'postgres',
    'host': 'localhost',
    'port': 5432,
}

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "new_scenarios"

db_pool = None
producer = None

class ScenarioRequest(BaseModel):
    vid_id: int

class StatusUpdateRequest(BaseModel):
    status: str

@app.on_event("startup")
async def startup():
    global db_pool, producer
    db_pool = await asyncpg.create_pool(**DB_CONFIG)
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    await producer.start()



# === POST /scenario/ ===
@app.post("/scenario/")
async def create_scenario(req: ScenarioRequest):
    vid_id = req.vid_id
    new_id = str(uuid.uuid4())
    status = "init_startup"
    predict = f"prediction_for_{new_id}"

    async with db_pool.acquire() as conn:
        try:
            await conn.execute("""
                INSERT INTO scenarios (id, status, vid_id, predict)
                VALUES ($1, $2, $3, $4);
            """, new_id, status, vid_id, predict)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    await producer.send_and_wait(KAFKA_TOPIC, {
        "type": "new_scenario",
        "scenario_id": new_id
    })

    return {
        "id": new_id,
        "status": status,
        "vid_id": vid_id,
        "predict": predict
    }

# === POST /scenario/{scenario_id}/ ===
@app.post("/scenario/{scenario_id}/")
async def update_scenario_status(scenario_id: str, req: StatusUpdateRequest):
    async with db_pool.acquire() as conn:
        result = await conn.execute("""
            UPDATE scenarios SET status = $1 WHERE id = $2
        """, req.status, scenario_id)

        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="Scenario not found")

    return {"scenario_id": scenario_id, "new_status": req.status}

# === GET /scenario/{scenario_id}/ ===
@app.get("/scenario/{scenario_id}/")
async def get_scenario_status(scenario_id: str):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT status FROM scenarios WHERE id = $1
        """, scenario_id)

        if not row:
            raise HTTPException(status_code=404, detail="Scenario not found")

        return {"scenario_id": scenario_id, "status": row["status"]}

# === GET /prediction/{scenario_id}/ ===
@app.get("/prediction/{scenario_id}/")
async def get_prediction(scenario_id: str):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT predict FROM scenarios WHERE id = $1
        """, scenario_id)

        if not row:
            raise HTTPException(status_code=404, detail="Scenario not found")

        return {"scenario_id": scenario_id, "prediction": row["predict"]}

# === Запуск через `python api.py` ===
if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=False)
