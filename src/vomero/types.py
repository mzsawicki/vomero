import typing

IdType: typing.TypeAlias = typing.Union[str, bytes]
Field: typing.TypeAlias = typing.Union[bytes, memoryview, str, int, float]
Event: typing.TypeAlias = typing.Dict[Field, Field]
ProducerCoro: typing.TypeAlias = typing.Callable[..., typing.Awaitable[Event]]
ConsumerCoro: typing.TypeAlias = typing.Callable[..., typing.Awaitable[typing.Any]]
