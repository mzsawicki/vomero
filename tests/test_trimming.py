import typing
import uuid

from src.vomero import Event, Streams

streams = Streams(decode_responses=True)


@streams.producer(stream="trimming-test-stream", max_len=10, max_len_approximate=False)
async def send_random_event() -> Event:
    return {"random_guid": str(uuid.uuid4())}


@streams.consumer(
    stream="trimming-test-stream",
    consumer_group="trimming-test-group",
    consumer="trimming-test-consumer",
    block=1,
)
async def read_event(event: typing.Optional[Event] = None) -> Event:
    return event


async def test_max_len_trimming():
    await streams.open()
    await streams.flush_all()
    await streams.create_consumer_group("trimming-test-stream", "trimming-test-group")
    for _ in range(25):
        await send_random_event()

    events_read_count = 0
    for _ in range(25):
        event = await read_event()
        if event is not None:
            events_read_count += 1

    await streams.close()
    assert events_read_count == 10


async def test_manual_max_len_trimming():
    await streams.open()
    await streams.flush_all()
    await streams.create_consumer_group("trimming-test-stream", "trimming-test-group")
    for _ in range(10):
        await send_random_event()

    await streams.trim_to_max_len("trimming-test-stream", max_len=3, approximate=False)

    events_read_count = 0
    for _ in range(10):
        event = await read_event()
        if event is not None:
            events_read_count += 1

    await streams.close()
    assert events_read_count == 3


async def test_manual_min_id_trimming():
    await streams.open()
    await streams.flush_all()
    await streams.create_consumer_group("trimming-test-stream", "trimming-test-group")
    for _ in range(10):
        await send_random_event()

    events = await streams.get_events_range("trimming-test-stream")
    id_to_trim_to, _ = events[5]
    await streams.trim_to_min_id(
        "trimming-test-stream", id_to_trim_to, approximate=False
    )

    events_read_count = 0
    for _ in range(10):
        event = await read_event()
        if event is not None:
            events_read_count += 1

    await streams.close()
    assert events_read_count == 5
