import asyncio
import functools
import signal
import typing

from src.vomero.types import ConsumerCoro


class Worker:
    def __init__(
        self,
        coro_partial: functools.partial[typing.Any],
        loop: asyncio.AbstractEventLoop,
    ):
        self._coro = coro_partial
        self._stopped = False
        self._loop = loop

    async def run(self) -> None:
        self._loop.add_signal_handler(signal.Signals.SIGINT, self.stop)
        self._loop.add_signal_handler(signal.Signals.SIGTERM, self.stop)
        await self._loop.create_task(self._loop_till_stopped())

    def stop(self) -> None:
        self._stopped = True

    async def _loop_till_stopped(self) -> None:
        while not self._stopped:
            await self._coro()


async def run_as_worker(
    coro: ConsumerCoro, /, *args: typing.Any, **kwargs: typing.Any
) -> None:
    event_loop = asyncio.get_event_loop()
    worker = Worker(functools.partial(coro, *args, **kwargs), event_loop)
    await worker.run()
