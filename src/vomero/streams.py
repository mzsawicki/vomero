import functools
import typing

import redis.asyncio as redis

Field: typing.TypeAlias = typing.Union[bytes, memoryview, str, int, float]
Event: typing.TypeAlias = typing.Dict[Field, Field]
ProducerCoro: typing.TypeAlias = typing.Callable[..., typing.Awaitable[Event]]
ConsumerCoro: typing.TypeAlias = typing.Callable[..., ...]


MAX_STREAM_LEN_DEFAULT = 1024
AUTO_CLAIM_TIMEOUT_DEFAULT = 60


class Streams:
    def __init__(self, **kwargs):
        self._redis = redis.Redis(**kwargs)

    async def create_consumer_group(
        self, stream: str, consumer_group_name: str, last_id: int = 0
    ) -> None:
        await self._redis.xgroup_create(
            stream, consumer_group_name, last_id, mkstream=True
        )

    def producer(
        self,
        stream: str,
        max_len: int = MAX_STREAM_LEN_DEFAULT,
        max_len_approximate: bool = True,
    ) -> typing.Callable[[ProducerCoro], ProducerCoro]:
        def wrapper_decorator(producer_coro: ProducerCoro) -> ProducerCoro:
            @functools.wraps(producer_coro)
            async def wrapper_producer(*args, **kwargs) -> Event:
                event = await producer_coro(*args, **kwargs)
                await self._produce_event(stream, max_len, max_len_approximate, event)
                return event

            return wrapper_producer

        return wrapper_decorator

    def consumer(
        self,
        stream: str,
        consumer_group: str,
        consumer: str,
        block: int = 0,
        auto_claim: bool = False,
        auto_claim_timeout: int = AUTO_CLAIM_TIMEOUT_DEFAULT,
    ) -> typing.Callable[[ConsumerCoro], ConsumerCoro]:
        def wrapper_decorator(consumer_coro: ConsumerCoro) -> ConsumerCoro:
            @functools.wraps(consumer_coro)
            async def wrapper_consumer(*args, **kwargs) -> typing.Any:
                event_received = False
                if auto_claim:
                    response = await self._auto_claim_pending_entry(
                        stream, consumer_group, consumer, auto_claim_timeout
                    )
                    _, record, _ = response
                    if record:
                        id_, event = record.pop()
                        event_received = True
                if not event_received:
                    response = await self._read_next_entry(
                        stream, consumer_group, consumer, block
                    )
                    if response:
                        record = response.pop()
                        _, entry = record
                        id_, event = entry.pop()
                        event_received = True
                if event_received:
                    coro_result = await consumer_coro(event, *args, **kwargs)
                    await self._acknowledge(stream, consumer_group, id_)
                else:
                    coro_result = await consumer_coro(*args, **kwargs)

                return coro_result

            return wrapper_consumer

        return wrapper_decorator

    async def trim_to_max_len(
        self, stream: str, max_len: int, approximate: bool = True
    ) -> None:
        await self._redis.xtrim(stream, maxlen=max_len, approximate=approximate)

    async def trim_to_min_id(
        self, stream: str, min_id: str, approximate: bool = True
    ) -> None:
        await self._redis.xtrim(stream, minid=min_id, approximate=approximate)

    async def open(self) -> None:
        await self._redis.initialize()

    async def close(self) -> None:
        await self._redis.close()

    async def remove_consumer_group(self, stream: str, consumer_group: str) -> None:
        await self._redis.xgroup_destroy(stream, consumer_group)

    async def get_events_range(
        self,
        stream: str,
        start: typing.Optional[str] = None,
        end: typing.Optional[str] = None,
    ) -> typing.List[typing.Tuple[typing.Union[str, bytes], Event]]:
        start_id = start or "-"
        end_id = end or "+"
        response = await self._redis.xrange(stream, start_id, end_id)
        return response

    async def get_pending_events_count(self, stream: str, consumer_group: str) -> int:
        response = await self._redis.xpending(stream, consumer_group)
        return response["pending"]

    async def flush_all(self) -> None:
        await self._redis.flushall()

    async def _produce_event(
        self, stream: str, max_len: int, max_len_approximate: bool, event: Event
    ) -> None:
        await self._redis.xadd(
            stream, event, maxlen=max_len, approximate=max_len_approximate
        )

    async def _auto_claim_pending_entry(
        self, stream: str, consumer_group: str, consumer: str, timeout: int
    ) -> typing.List[typing.Any]:
        entry = await self._redis.xautoclaim(
            stream, consumer_group, consumer, timeout, count=1
        )
        return entry

    async def _read_next_entry(
        self, stream: str, consumer_group: str, consumer: str, block: int
    ) -> typing.List[typing.Any]:
        entry = await self._redis.xreadgroup(
            consumer_group, consumer, {stream: ">"}, count=1, block=block
        )
        return entry

    async def _acknowledge(
        self, stream: str, consumer_group: str, entry_id: bytes
    ) -> None:
        await self._redis.xack(stream, consumer_group, entry_id)
