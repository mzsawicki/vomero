import datetime
import typing

from src.vomero import Streams, Event

streams = Streams(decode_responses=True)


@streams.producer("test-stream")
async def send_message(message: str) -> Event:
    return {
        "time": str(datetime.datetime(2024, 2, 22, 12)),
        "message": message,
        "author": "Test author",
    }


@streams.consumer(
    stream="test-stream", consumer_group="test-group", consumer="test-consumer"
)
async def consume_and_fail(event: typing.Optional[Event] = None) -> None:
    raise KeyError


@streams.consumer(
    stream="test-stream", consumer_group="test-group", consumer="test-consumer"
)
async def consume_and_succeed(event: typing.Optional[Event] = None) -> str:
    return event["message"]


async def test_failure_and_recovery():
    await streams.remove_consumer_group("test-stream", "test-group")
    await streams.create_consumer_group("test-stream", "test-group")
    await send_message("Failing message")
    try:
        await consume_and_fail()
    except KeyError:
        pass
    message = await consume_and_succeed()
    await streams.close()
    assert message == "Failing message"
