import datetime
import typing

from src.vomero import Event, Streams

streams = Streams(decode_responses=True)


@streams.producer(stream="claiming-test-stream")
async def produce():
    return {"message": "To be claimed"}


@streams.consumer(
    stream="claiming-test-stream",
    consumer_group="claiming-test-group",
    consumer="failing-consumer",
)
async def consume_and_fail(event: typing.Optional[Event] = None) -> None:
    if event:
        raise KeyError


@streams.consumer(
    stream="claiming-test-stream",
    consumer_group="claiming-test-group",
    consumer="success-consumer",
    auto_claim=True,
    auto_claim_timeout=60000,
    block=1,
)
async def consume_and_succeed(event: typing.Optional[Event] = None) -> bool:
    if event is not None:
        return True
    else:
        return False


async def test_claiming_long_pending_event():
    await streams.open()
    await streams.flush_all()
    await streams.create_consumer_group("claiming-test-stream", "claiming-test-group")

    start_time = datetime.datetime.now()

    await produce()
    first_consumer_failed = False
    try:
        await consume_and_fail()
    except KeyError:
        first_consumer_failed = True

    event_consumed = False
    while not event_consumed:
        event_consumed = await consume_and_succeed()

    stop_time = datetime.datetime.now()
    time_elapsed = (stop_time - start_time).seconds

    pending_events_count = await streams.get_pending_events_count(
        "claiming-test-stream", "claiming-test-group"
    )
    are_there_pending_events = pending_events_count > 0

    minute_elapsed = time_elapsed >= 60

    await streams.close()

    success = (
        first_consumer_failed
        and event_consumed
        and not are_there_pending_events
        and minute_elapsed
    )
    assert success
