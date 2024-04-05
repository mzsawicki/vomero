import functools
import typing

import redis.asyncio as redis

from src.vomero.types import IdType, ProducerCoro, ConsumerCoro, Event

MAX_STREAM_LEN_DEFAULT = 1024
AUTO_CLAIM_TIMEOUT_DEFAULT = 60


class Streams:
    def __init__(self, **kwargs: typing.Any):
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
            async def wrapper_producer(
                *args: typing.Any, **kwargs: typing.Any
            ) -> Event:
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
            async def wrapper_consumer(
                *args: typing.Any, **kwargs: typing.Any
            ) -> typing.Any:
                id_, event = None, None
                if auto_claim:
                    id_, event = await self._auto_claim_pending_entry(
                        stream, consumer_group, consumer, auto_claim_timeout
                    )
                if id_ is None:
                    id_, event = await self._read_next_entry(
                        stream, consumer_group, consumer, block
                    )
                if id_ and event:
                    event = _add_metadata_to_event(
                        id_, event, stream, consumer_group, consumer
                    )
                coro_result = await consumer_coro(event, *args, **kwargs)
                if id_ is not None:
                    await self._acknowledge(stream, consumer_group, id_)

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
        await self._redis.aclose()

    async def remove_consumer_group(self, stream: str, consumer_group: str) -> None:
        await self._redis.xgroup_destroy(stream, consumer_group)

    async def get_events_range(
        self,
        stream: str,
        start: typing.Optional[str] = None,
        end: typing.Optional[str] = None,
    ) -> typing.Any:
        start_id = start or "-"
        end_id = end or "+"
        response = await self._redis.xrange(stream, start_id, end_id)
        return response

    async def get_pending_events_count(self, stream: str, consumer_group: str) -> int:
        response = await self._redis.xpending(stream, consumer_group)
        return int(response["pending"])

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
    ) -> typing.Tuple[typing.Optional[IdType], typing.Optional[Event]]:
        response = await self._redis.xautoclaim(
            stream, consumer_group, consumer, timeout, count=1
        )
        _, record, _ = response
        if record:
            id_, event = record.pop()
            return id_, event
        else:
            return None, None

    async def _read_next_entry(
        self, stream: str, consumer_group: str, consumer: str, block: int
    ) -> typing.Tuple[typing.Optional[IdType], typing.Optional[Event]]:
        response = await self._redis.xreadgroup(
            consumer_group, consumer, {stream: ">"}, count=1, block=block
        )
        if response:
            record = response.pop()
            _, entry = record
            id_, event = entry.pop()
            return id_, event
        else:
            return None, None

    async def _acknowledge(
        self, stream: str, consumer_group: str, entry_id: IdType
    ) -> None:
        await self._redis.xack(stream, consumer_group, entry_id)


def _add_metadata_to_event(
    id_: IdType, event: Event, stream: str, consumer_group: str, consumer: str
) -> Event:
    enriched_event = {
        "_id": id_,
        "_stream": stream,
        "_consumer_group": consumer_group,
        "_consumer": consumer,
        **event,
    }
    return enriched_event
