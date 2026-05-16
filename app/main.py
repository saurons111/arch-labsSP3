import os
import pika
import time
from fastapi.responses import Response
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
import json
from datetime import datetime
import redis
from typing import List

from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Boolean, Float, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker, Session


DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "archdb")
DB_USER = os.getenv("DB_USER", "archuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "archpass")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True
)

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

TASK_EXCHANGE = "task.exchange"
TASK_QUEUE = "task.events"
TASK_ROUTING_KEY = "task.created"

DATABASE_URL = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()

app = FastAPI(title="Architecture Labs Task Service")

TASKS_CREATED_TOTAL = Counter(
    "tasks_created_total",
    "Total number of created tasks"
)

HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total number of HTTP requests",
    ["method", "endpoint"]
)

TASK_REQUEST_DURATION_SECONDS = Histogram(
    "task_request_duration_seconds",
    "Task request duration in seconds",
    ["method", "endpoint"]
)

TASKS_IN_DATABASE = Gauge(
    "tasks_in_database",
    "Current number of tasks in database"
)

class Task(Base):
    __tablename__ = "tasks"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, nullable=False)
    description = Column(String, nullable=True)
    completed = Column(Boolean, default=False)


class BalanceEvent(Base):
    __tablename__ = "balance_events"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, index=True, nullable=False)
    event_type = Column(String, nullable=False)
    amount = Column(Float, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class BalanceView(Base):
    __tablename__ = "balance_view"

    user_id = Column(String, primary_key=True, index=True)
    balance = Column(Float, nullable=False, default=0.0)


class BalanceHistoryEntry(Base):
    __tablename__ = "balance_history"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, index=True, nullable=False)
    operation_type = Column(String, nullable=False)
    amount = Column(Float, nullable=True)
    balance_after = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class TaskCreate(BaseModel):
    title: str
    description: str | None = None


class TaskRead(BaseModel):
    id: int
    title: str
    description: str | None
    completed: bool

    class Config:
        from_attributes = True


class BalanceAmountCommand(BaseModel):
    amount: float


class BalanceRead(BaseModel):
    user_id: str
    balance: float

    class Config:
        from_attributes = True


class BalanceHistoryRead(BaseModel):
    id: int
    user_id: str
    operation_type: str
    amount: float | None
    balance_after: float
    created_at: datetime

    class Config:
        from_attributes = True


class BalanceEventRead(BaseModel):
    id: int
    user_id: str
    event_type: str
    amount: float | None
    created_at: datetime

    class Config:
        from_attributes = True


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()



## Записывает событие в Event Store
def append_balance_event(
    db: Session,
    user_id: str,
    event_type: str,
    amount: float | None = None
):
    event = BalanceEvent(
        user_id=user_id,
        event_type=event_type,
        amount=amount
    )

    db.add(event)
    db.commit()
    db.refresh(event)

    return event

##берёт все события пользователя и заново строит read-модели:
def rebuild_balance_projection(user_id: str, db: Session):
    events = (
        db.query(BalanceEvent)
        .filter(BalanceEvent.user_id == user_id)
        .order_by(BalanceEvent.id)
        .all()
    )

    db.query(BalanceView).filter(BalanceView.user_id == user_id).delete()
    db.query(BalanceHistoryEntry).filter(BalanceHistoryEntry.user_id == user_id).delete()
    db.commit()

    current_balance = 0.0

    for event in events:
        if event.event_type == "BALANCE_CREATED":
            current_balance = 0.0
            operation_type = "CREATE"

        elif event.event_type == "BALANCE_CREDITED":
            current_balance += event.amount
            operation_type = "CREDIT"

        elif event.event_type == "BALANCE_DEBITED":
            current_balance -= event.amount
            operation_type = "DEBIT"

        else:
            continue

        history_entry = BalanceHistoryEntry(
            user_id=user_id,
            operation_type=operation_type,
            amount=event.amount,
            balance_after=current_balance,
            created_at=event.created_at
        )

        db.add(history_entry)

    balance_view = BalanceView(
        user_id=user_id,
        balance=current_balance
    )

    db.add(balance_view)
    db.commit()


@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=engine)


@app.get("/")
def root():
    
    return {
        "message": "Architecture Labs API is running",
        "lab": "Lab 1 Docker + PostgreSQL"
    }


@app.get("/metrics")
def metrics():
    return Response(
        generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )


##функция которая будет отправлять событие в RABBITMQ
def publish_task_created_event(task: Task):
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)

    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials
    )

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.exchange_declare(
        exchange=TASK_EXCHANGE,
        exchange_type="topic",
        durable=True
    )

    channel.queue_declare(
        queue=TASK_QUEUE,
        durable=True
    )

    channel.queue_bind(
        exchange=TASK_EXCHANGE,
        queue=TASK_QUEUE,
        routing_key="task.*"
    )

    event = {
        "event": "task.created",
        "id": task.id,
        "title": task.title,
        "description": task.description,
        "completed": task.completed
    }

    channel.basic_publish(
        exchange=TASK_EXCHANGE,
        routing_key=TASK_ROUTING_KEY,
        body=json.dumps(event),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json"
        )
    )

    connection.close()




@app.post("/tasks", response_model=TaskRead)
def create_task(task: TaskCreate, db: Session = Depends(get_db)):
    start_time = time.time()

    new_task = Task(
        title=task.title,
        description=task.description,
        completed=False
    )
    db.add(new_task)
    db.commit()
    db.refresh(new_task)

    TASKS_CREATED_TOTAL.inc()
    HTTP_REQUESTS_TOTAL.labels(method="POST", endpoint="/tasks").inc()
    TASK_REQUEST_DURATION_SECONDS.labels(
        method="POST",
        endpoint="/tasks"
    ).observe(time.time() - start_time)

    tasks_count = db.query(Task).count()
    TASKS_IN_DATABASE.set(tasks_count)

    publish_task_created_event(new_task)

    return new_task


@app.get("/tasks", response_model=List[TaskRead])
def get_tasks(db: Session = Depends(get_db)):
    return db.query(Task).all()


@app.get("/tasks/{task_id}", response_model=TaskRead)
def get_task(task_id: int, db: Session = Depends(get_db)):
    cache_key = f"task:{task_id}"

    cached_task = redis_client.get(cache_key)
    if cached_task:
        return json.loads(cached_task)

    task = db.query(Task).filter(Task.id == task_id).first()

    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")

    task_data = {
        "id": task.id,
        "title": task.title,
        "description": task.description,
        "completed": task.completed
    }

##333
    redis_client.setex(cache_key, 60, json.dumps(task_data))

    return task_data


@app.delete("/tasks/{task_id}")
def delete_task(task_id: int, db: Session = Depends(get_db)):
    task = db.query(Task).filter(Task.id == task_id).first()

    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")

    db.delete(task)
    db.commit()

    redis_client.delete(f"task:{task_id}")

    return {"message": "Task deleted"}



# =========================
# Lab 5 — CQRS Command Side
# =========================
#/create → создаёт событие BALANCE_CREATED
#/credit → создаёт событие BALANCE_CREDITED
#/debit → создаёт событие BALANCE_DEBITED


@app.post("/balances/{user_id}/create")
def create_balance(user_id: str, db: Session = Depends(get_db)):
    existing_events = (
        db.query(BalanceEvent)
        .filter(BalanceEvent.user_id == user_id)
        .count()
    )

    if existing_events > 0:
        raise HTTPException(status_code=400, detail="Balance already exists")

    append_balance_event(
        db=db,
        user_id=user_id,
        event_type="BALANCE_CREATED",
        amount=None
    )

    rebuild_balance_projection(user_id, db)

    return {
        "message": "Balance created",
        "user_id": user_id
    }


@app.post("/balances/{user_id}/credit")
def credit_balance(
    user_id: str,
    command: BalanceAmountCommand,
    db: Session = Depends(get_db)
):
    if command.amount <= 0:
        raise HTTPException(status_code=400, detail="Amount must be greater than zero")

    balance = db.query(BalanceView).filter(BalanceView.user_id == user_id).first()

    if balance is None:
        raise HTTPException(status_code=404, detail="Balance not found")

    append_balance_event(
        db=db,
        user_id=user_id,
        event_type="BALANCE_CREDITED",
        amount=command.amount
    )

    rebuild_balance_projection(user_id, db)

    return {
        "message": "Balance credited",
        "user_id": user_id,
        "amount": command.amount
    }


@app.post("/balances/{user_id}/debit")
def debit_balance(
    user_id: str,
    command: BalanceAmountCommand,
    db: Session = Depends(get_db)
):
    if command.amount <= 0:
        raise HTTPException(status_code=400, detail="Amount must be greater than zero")

    balance = db.query(BalanceView).filter(BalanceView.user_id == user_id).first()

    if balance is None:
        raise HTTPException(status_code=404, detail="Balance not found")

    if balance.balance < command.amount:
        raise HTTPException(status_code=400, detail="Insufficient funds")

    append_balance_event(
        db=db,
        user_id=user_id,
        event_type="BALANCE_DEBITED",
        amount=command.amount
    )

    rebuild_balance_projection(user_id, db)

    return {
        "message": "Balance debited",
        "user_id": user_id,
        "amount": command.amount
    }



# =========================
# Lab 5 — CQRS Query Side
# =========================


#/balances/{user_id}          → читает read-модель текущего баланса
#/balances/{user_id}/history  → читает read-модель истории
#/balances/{user_id}/events   → показывает Event Store

@app.get("/balances/{user_id}", response_model=BalanceRead)
def get_balance(user_id: str, db: Session = Depends(get_db)):
    balance = db.query(BalanceView).filter(BalanceView.user_id == user_id).first()

    if balance is None:
        raise HTTPException(status_code=404, detail="Balance not found")

    return balance


@app.get("/balances/{user_id}/history", response_model=List[BalanceHistoryRead])
def get_balance_history(user_id: str, db: Session = Depends(get_db)):
    return (
        db.query(BalanceHistoryEntry)
        .filter(BalanceHistoryEntry.user_id == user_id)
        .order_by(BalanceHistoryEntry.id)
        .all()
    )


@app.get("/balances/{user_id}/events", response_model=List[BalanceEventRead])
def get_balance_events(user_id: str, db: Session = Depends(get_db)):
    return (
        db.query(BalanceEvent)
        .filter(BalanceEvent.user_id == user_id)
        .order_by(BalanceEvent.id)
        .all()
    )