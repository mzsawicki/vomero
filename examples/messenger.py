import asyncio
import uuid

import datetime

import aioconsole

from src.vomero import Streams, Event

streams = Streams(decode_responses=True)

user_id = str(uuid.uuid4())
user_name = "John Doe"


@streams.producer(stream="messages")
async def send_message(user: str, message: str) -> Event:
    return {
        "user": user,
        "message": message,
        "time": str(datetime.datetime.now())
    }


@streams.consumer(stream="messages", consumer_group=user_id, consumer=user_id)
async def print_message(event: Event) -> None:
    user = event["user"]
    message = event["message"]
    time = event["time"]
    await aioconsole.aprint(f"[{time}] {user} says: {message}\r")


async def async_input():
    while True:
        input_ = await aioconsole.ainput()
        await send_message(user_name, input_)


async def read_messages():
    while True:
        await print_message()


async def main():
    await aioconsole.aprint(f"Chatting as {user_name}")
    await streams.create_consumer_group("messages", user_id)
    await asyncio.gather(read_messages(), async_input())


asyncio.run(main())
