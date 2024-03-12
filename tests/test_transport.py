from src.vomero import Streams, Event


streams = Streams()


@streams.producer(stream="example-stream")
async def send_message(message: str) -> Event:
    return {"message": message}


@streams.consumer(
    stream="example-stream", consumer_group="example-group", consumer="example-consumer"
)
async def print_message(event: Event) -> None:
    print(event)


async def test_transport():
    # await client.create_consumer_group("example-stream", "example-group")
    await send_message("Hello World!")
    await print_message()
    await streams.close()
