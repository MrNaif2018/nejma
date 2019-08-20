import re
import random
import string
import time
# optional dependency
try:
    import aioredis
except ImportError:
    aioredis = None


class Channel:
    def __init__(self, name=None, send=None, expires=60):
        if name:
            assert self.validate_name(name), "Invalid channel name"
        self.name = name or "".join(random.choices(string.ascii_letters, k=12))
        self.expires = expires
        self.created_at = time.time()
        self._send = send

    async def send(self, message):
        await self._send(message)

    def validate_name(self, name):
        if name.isidentifier():
            return True
        raise TypeError(
            "Channels names must be valid python identifier"
            + "only alphanumerics and underscores are accepted"
        )

    def is_expired(self):
        return self.expires + int(self.created_at) < time.time()

    def __repr__(self):
        return f'<{self.__class__.__name__} name="{self.name}">'


class ChannelLayer:
    def __init__(self, expires=36000, capacity=100):
        self.capacity = capacity
        self.expires = expires

        self.groups = {}

    async def add(self, group_name, channel, send=None):
        assert self.validate_name(group_name), "Invalid group name"
        if isinstance(channel, (str, bytes)):
            channel = Channel(name=channel, send=send)

        self.groups.setdefault(group_name, {})
        # lookup
        self.groups[group_name][channel] = 1

    async def remove(self, group_name, channel):
        if group_name in self.groups:
            if channel in self.groups[group_name]:
                del self.groups[group_name][channel]

    async def flush(self):
        self.groups = {}

    async def group_send(self, group, payload):
        await self.clean_expired()
        for channel in self.groups.get(group, {}):
            await channel.send(payload)

    async def remove_channel(self, channel):
        for group in self.groups:
            if channel in self.groups[group]:
                del self.groups[group][channel]

    def validate_name(self, name):
        if name.isidentifier():
            return True
        raise TypeError(
            "Group names must be valid python identifier"
            + "only alphanumerics and underscores are accepted"
        )

    async def clean_expired(self):
        for group in self.groups:
            # Can't change dict size during iteration
            for channel in list(self.groups.get(group, {})):
                if channel.is_expired():
                    del self.groups[group][channel]


class RedisLayer(ChannelLayer):
    def __init__(self, redis_host="redis://localhost", prefix="group"):
        self.initialized = False
        self.send = None
        self.redis_host = redis_host
        self.prefix = prefix

    ### utility functions ###

    def group_prefix(self, group):
        group = str(group)
        return f"{self.prefix}_{group}" if not group.startswith(
            f"{self.prefix}_") else f"{group}"

    async def _initialize(self):
        self.redis: aioredis.Redis = await aioredis.create_redis_pool(self.redis_host)
        self.initialized = True

    ### add/remove interface ###

    async def add(self, group_name, channel, send=None):
        if not self.initialized:
            await self._initialize()
        assert self.validate_name(group_name), "Invalid group name"
        if isinstance(channel, Channel):
            channel = channel.name
        group_name = self.group_prefix(group_name)
        await self.redis.hset(group_name, channel, 1)

    async def remove_channel(self, channel):
        if not self.initialized:
            await self._initialize()
        if isinstance(channel, Channel):
            channel = channel.name
        _, keys = await self.redis.scan(match=self.group_prefix("*"))
        for key in keys:
            await self.redis.hdel(key, channel)

    async def remove(self, group_name, channel):
        if not self.initialized:
            await self._initialize()
        group_name = self.group_prefix(group_name)
        await self.redis.hdel(group_name, channel)

    async def flush(self):
        if not self.initialized:
            await self._initialize()
        _, keys = await self.redis.scan(match=self.group_prefix("*"))
        for key in keys:
            await self.redis.delete(key)

    ### send interface ###

    async def group_send(self, group, payload):
        if not self.initialized:
            await self._initialize()
        group_name = self.group_prefix(group)
        for _ in await self.redis.hgetall(group_name):
            await Channel(send=self.send).send(payload)


channel_layer = ChannelLayer()
