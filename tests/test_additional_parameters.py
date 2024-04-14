import typing

from src.vomero import Event, Streams

streams = Streams(decode_responses=True)


@streams.producer(stream="additional-parameters")
async def produce() -> Event:
    return {"message": "Sample message"}


@streams.consumer(
    stream="additional-parameters",
    consumer_group="example-group",
    consumer="example-consumer",
)
async def consume_with_additional_parameters(
    event: typing.Optional[Event], arg_: int, kwarg_: str = "abc"
) -> typing.Tuple[int, str]:
    if event:
        return arg_, kwarg_
    else:
        raise ValueError


async def test_additional_parameters():
    await streams.open()
    await streams.flush_all()
    await streams.create_consumer_group("additional-parameters", "example-group")
    await produce()
    result = await consume_with_additional_parameters(1, kwarg_="xyz")
    await streams.close()
    assert result == (1, "xyz")
