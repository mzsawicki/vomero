import typing

from src.vomero import Event, EventStream


class ExampleEvent(Event):
    def __init__(self, message: str):
        self._message = message

    def serialize(self) -> typing.Dict[str, typing.Any]:
        return {"message": self._message}

    @classmethod
    def deserialize(cls, dict_: typing.Dict[str, typing.Any]):
        return ExampleEvent(dict_["message"])


stream = EventStream("example_stream")


@stream.producer
async def send_message(message: str) -> Event:
    return ExampleEvent(message)


async def test_transport():
    await send_message("Hello World!")
    await stream.close()