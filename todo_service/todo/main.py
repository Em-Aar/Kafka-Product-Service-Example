from typing import Annotated
from fastapi import Depends, FastAPI, BackgroundTasks
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from todo import setting
from sqlmodel import SQLModel
import json
from todo import todo_pb2


class AddTodo (SQLModel):
    content: str


async def create_topic():
    admin_client = AIOKafkaAdminClient(
        bootstrap_servers=setting.BOOTSTRAP_SERVER)
    await admin_client.start()
    topic_list = [NewTopic(name=setting.KAFKA_ORDER_TOPIC,
                           num_partitions=2, replication_factor=1)]
    try:
        await admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic '{setting.KAFKA_ORDER_TOPIC}' created successfully")
    except Exception as e:
        print(f"Failed to create topic '{setting.KAFKA_ORDER_TOPIC}': {e}")
    finally:
        await admin_client.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Fastapi app started...")
    await create_topic()
    yield


app = FastAPI(lifespan=lifespan,
              title="FastAPI Producer Service...",
              version='1.0.0'
              )


@app.get('/')
async def root():
    return {"message": "Welcome to the todo microservice"}


async def kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers=setting.BOOTSTRAP_SERVER)
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()


@app.post('/create_todo', response_model=AddTodo)
async def create_todo(
    todo: AddTodo,
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)]
):

    # serialized_todo = json.dumps(todo.__dict__).encode('utf-8')
    todo_proto = todo_pb2.Todo()
    todo_proto.content = todo.content
    serialized_todo = todo_proto.SerializeToString()
    await producer.send_and_wait(setting.KAFKA_ORDER_TOPIC, serialized_todo)

    return todo
