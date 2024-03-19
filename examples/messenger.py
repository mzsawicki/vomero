import asyncio
import uuid
import datetime
import typing

import aioconsole

from src.vomero import Streams, Event, run_as_worker

streams = Streams(decode_responses=True)

user_id = str(uuid.uuid4())


@streams.producer(stream="messages")
async def send_message(user: str) -> Event:
    input_ = await aioconsole.ainput()
    return {"user": user, "message": input_, "time": str(datetime.datetime.now())}


@streams.consumer(stream="messages", consumer_group=user_id, consumer=user_id, block=1000)
async def print_message(event: typing.Optional[Event] = None) -> None:
    if event:
        user = event["user"]
        message = event["message"]
        time = event["time"]
        await aioconsole.aprint(f"[{time}] {user} says: {message}", end="\n")


async def main():
    user_name = await aioconsole.ainput("Choose your name: > ")
    await aioconsole.aprint(f"Chatting as {user_name}")
    await streams.create_consumer_group("messages", user_id)
    await asyncio.gather(
        run_as_worker(print_message),
        run_as_worker(send_message, user_name)
    )
    await streams.close()


asyncio.run(main())
