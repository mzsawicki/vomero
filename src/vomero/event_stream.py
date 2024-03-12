import functools
import typing

import redis.asyncio as redis

Field: typing.TypeAlias = typing.Union[bytes, memoryview, str, int, float]
Event: typing.TypeAlias = typing.Dict[Field, Field]
ProducerCoro: typing.TypeAlias = typing.Callable[..., typing.Awaitable[Event]]
ConsumerCoro: typing.TypeAlias = typing.Callable[[Event, ...], ...]


class Streams:
    def __init__(self, **kwargs):
        self._redis = redis.Redis(**kwargs)

    async def create_consumer_group(self, stream: str, consumer_group_name: str, last_id: int = 0) -> None:
        await self._redis.xgroup_create(stream, consumer_group_name, last_id, mkstream=True)

    def producer(self, stream: str) -> typing.Callable[[ProducerCoro],ProducerCoro]:
        def wrapper_decorator(producer_coro: ProducerCoro) -> ProducerCoro:
            @functools.wraps(producer_coro)
            async def wrapper_producer(*args, **kwargs) -> Event:
                event = await producer_coro(*args, **kwargs)
                await self._produce_event(stream, event)
                return event

            return wrapper_producer

        return wrapper_decorator

    def consumer(self, stream: str, consumer_group: str, consumer: str, block: int = 0):
        def consumer_decorator(consumer_coro: ConsumerCoro) -> ConsumerCoro:
            @functools.wraps(consumer_coro)
            async def wrapper_consumer(*args, **kwargs) -> typing.Any:
                redis_response = await self._consume_event(stream, consumer_group, consumer, block)
                record = redis_response.pop()
                _, entry = record
                id_, event = entry.pop()
                coro_result = await consumer_coro(event, *args, **kwargs)
                await self._acknowledge(stream, consumer_group, id_)
                return coro_result

            return wrapper_consumer

        return consumer_decorator

    async def _produce_event(self, stream: str, event: Event) -> None:
        await self._redis.xadd(stream, event)

    async def _consume_event(self, stream: str, consumer_group: str, consumer: str, block: int = 0) -> typing.List:
        return await self._redis.xreadgroup(
            consumer_group,
            consumer,
            {stream: ">"},
            count=1,
            block=block
        )

    async def _acknowledge(self, stream: str, consumer_group: str, entry_id: bytes) -> None:
        await self._redis.xack(stream, consumer_group, entry_id)

    async def close(self) -> None:
        await self._redis.close()
