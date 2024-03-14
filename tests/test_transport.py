from src.vomero import Streams, Event


streams = Streams(decode_responses=True)


@streams.producer(stream="example-stream")
async def send_message(message: str) -> Event:
    return {"message": message}


@streams.consumer(
    stream="example-stream", consumer_group="example-group", consumer="example-consumer"
)
async def check_message(event: Event) -> None:
    assert event["message"] == "Hello World!"


async def test_transport():
    await streams.remove_consumer_group("example-stream", "example-group")
    await streams.create_consumer_group("example-stream", "example-group")
    await send_message("Hello World!")
    await check_message()
    await streams.close()
