import abc
import functools
import typing

import redis.asyncio as redis


class Event(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def serialize(self) -> typing.Dict[str, typing.Any]:
        pass

    @classmethod
    @abc.abstractmethod
    def deserialize(cls, dict_: typing.Dict[str, typing.Any]) -> typing.Self:
        pass


class EventStream:
    def __init__(self, stream: str, consumer_group: typing.Optional[str] = None, **kwargs):
        self._redis = redis.Redis(**kwargs)
        self._stream = stream
        self._consumer_group = consumer_group

    def producer(
        self,
        producer_func: typing.Callable[..., typing.Awaitable[Event]],
    ) -> typing.Callable[..., typing.Awaitable[Event]]:
        @functools.wraps(producer_func)
        async def wrapper_producer(*args, **kwargs) -> Event:
            event = await producer_func(*args, **kwargs)
            await self.produce_event(event)
            return event
        return wrapper_producer

    async def produce_event(self, event: Event) -> None:
        event_data = event.serialize()
        await self._redis.xadd(self._stream, event_data)

    async def close(self) -> None:
        await self._redis.close()
