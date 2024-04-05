# Vomero

Vomero is event processing mini-library based  on Redis Streams.
Currently still **work in progress**, although core features are already in place. The library is meant to not provide
too much abstraction over Redis, so it's advised to get familiar with key concepts of Redis Streams itself before usage.
## Features
- "At least one" processing semantics
- Auto-recovery for failed tasks using auto-claiming
- Easy scaling with consumer groups
- Support for async
- Strictly typed

## Limitations
Vomero does not provide strict ordering inside consumer groups, aiming to take maximum advantage of low delivery latency.
If strict ordering is a must for your project, you probably need other solution.

## Quickstart
### Key concepts
- **Event** is simply a sparse Python dict with some limitations. The dictionary values must be one of:
`bytes`, `memoryview`, `str`, `int`, `float`.
- **Producer** is an async function returning Event object. Returned Event is sent to the event stream.
- **Consumer** is an async function which gets at leas one optional argument: Event objects. The consumer defines how
the event is to be processed.
### Defining producers

```python
from vomero import Streams, Event

streams = Streams()

# Producer has to be defined as an async function returning Event
@streams.producer(stream="my-event-stream")
async def produce_event() -> Event:
    return {"content": "Hello world"}

```

### Defining consumers
```python
import typing

from vomero import Streams, Event

streams = Streams(decode_responses=True)

# Consumer has to be defined as async function which gets
# At least one optional argument (the event) and may return any type
@streams.consumer(
    stream="my-event-stream",
    consumer_group="my-consumer-group",
    consumer="my-consumer",
)
async def consume_and_print_event(event: typing.Optional[Event] = None) -> None:
    if event:
        content = event["content"]
        print(content)
```

### Running as worker
```python
import asyncio
import typing

from vomero import Streams, Event, run_as_worker

streams = Streams()

# The decorator's block parameter is recommended when running as worker
# To enable graceful stopping at SIGINT and SIGTERM signals
@streams.consumer(
    stream="my-event-stream",
    consumer_group="my-consumer-group",
    consumer="my-consumer",
    block=100
)
async def consume_event(event: typing.Optional[Event] = None) -> None:
    ...

if __name__ == "__main__":
    # Running a consumer as worker will call it in a loop
    # until SIGINT or SIGTERM is raised
    asyncio.run(run_as_worker(consume_event))
```