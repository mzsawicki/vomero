import uuid
from src.vomero import Streams, Event


streams = Streams(decode_responses=True)


def get_consumer_name() -> str:
    return str(uuid.uuid4())


@streams.producer(stream="example-stream")
async def send_message(message: str) -> Event:
    return {"message": message}


@streams.consumer(
    stream="example-stream",
    consumer_group="example-group",
    consumer_factory=get_consumer_name,
)
async def consume_and_return_consumer_name_1(event: Event) -> str:
    return event["_consumer"]


@streams.consumer(
    stream="example-stream",
    consumer_group="example-group",
    consumer_factory=get_consumer_name,
)
async def consume_and_return_consumer_name_2(event: Event) -> str:
    return event["_consumer"]


async def test_consumer_factory_same():
    await streams.open()
    await streams.flush_all()
    await streams.create_consumer_group("example-stream", "example-group")
    await send_message("Message 1")
    await send_message("Message 2")
    consumer_1 = await consume_and_return_consumer_name_1()
    consumer_2 = await consume_and_return_consumer_name_1()
    await streams.close()
    assert consumer_1 == consumer_2


async def test_consumer_factory_different():
    await streams.open()
    await streams.flush_all()
    await streams.create_consumer_group("example-stream", "example-group")
    await send_message("Message 1")
    await send_message("Message 2")
    consumer_1 = await consume_and_return_consumer_name_1()
    consumer_2 = await consume_and_return_consumer_name_2()
    await streams.close()
    assert consumer_1 != consumer_2
