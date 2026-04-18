import os
import json
import redis
from typing import List

from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Boolean
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

DATABASE_URL = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()

app = FastAPI(title="Architecture Labs Task Service")


class Task(Base):
    __tablename__ = "tasks"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, nullable=False)
    description = Column(String, nullable=True)
    completed = Column(Boolean, default=False)


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


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=engine)


@app.get("/")
def root():
    return {
        "message": "Architecture Labs API is running",
        "lab": "Lab 1 Docker + PostgreSQL"
    }


@app.post("/tasks", response_model=TaskRead)
def create_task(task: TaskCreate, db: Session = Depends(get_db)):
    new_task = Task(
        title=task.title,
        description=task.description,
        completed=False
    )
    db.add(new_task)
    db.commit()
    db.refresh(new_task)
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