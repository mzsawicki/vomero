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


class Client:
    def __init__(self, **kwargs):
        self._redis = redis.Redis(**kwargs)

    def producer(self, stream: str) -> typing.Callable[
        [typing.Callable[..., typing.Awaitable[Event]]],
        typing.Callable[..., typing.Awaitable[Event]],
    ]:
        def wrapper_decorator(
            producer_func: typing.Callable[..., typing.Awaitable[Event]]
        ) -> typing.Callable[..., typing.Awaitable[Event]]:
            @functools.wraps(producer_func)
            async def wrapper_producer(*args, **kwargs) -> Event:
                event = await producer_func(*args, **kwargs)
                await self.produce_event(stream, event)
                return event

            return wrapper_producer

        return wrapper_decorator

    async def produce_event(self, stream: str, event: Event) -> None:
        event_data = event.serialize()
        await self._redis.xadd(stream, event_data)

    async def close(self) -> None:
        await self._redis.close()
