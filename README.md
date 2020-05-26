## What ?

A redis client for Python

Please note that this project is currently alpha quality and the API is not finalized. Please provide feedback if you think the API is convenient enough or not. A permissive license will be chosen once the API will be more mature for wide spread consumption.

## Why ?

- Transparent API (Just call the redis commands, and the library will figure out cluster api, script caching, etc...)
- Cluster support
- RESP2/RESP3 support
- Per context / command properties (database #, decoding, encoding, RESP3 attributes, etc...)
- Pipelining
- Modular API allowing for support for multiple synchronous and (later) asynchronous event loops

## Roadmap

- Asynchronous I/O support with the same exact API (but with the await keyword), targeting asyncio and trio
- Transparent EVAL script caching
- Redis client-side caching

## Not on roadmap (for now?)

- Hiredis support
- Sentinal support
- High level features which are not part of the redis specification (such as locks, retry transactions, etc...)
- Manual command interface (maybe for special stuff like bit operations?)

## Installing

For now you can install this via this github repository by pip installing or adding to your requirements.txt file:

```
git+git://github.com/tzickel/justredis@master#egg=justredis
```

Replace master with the specific branch or version tag you want.

## Examples

TBD

## API

```python
Redis(**kwargs)
    @classmethod
    from_url(url, **kwargs)
    __enter__() / __exit__(*args)
    close()
    __call__(*cmd, **kwargs)
    endpoints()
    modify(**kwargs) # Returns a modified settings instance (while sharing the pool)
    connection(*args, push=False, **kwargs)
        __enter__() / __exit__(*args)
        close()
        __call__(*cmd, **kwargs) # On push connection no result for calls
        modify(**kwargs) # Returns a modified settings instance (while sharing the connection)

        # Push connection only commands
        next_message(timeout=None, **kwargs)
        __iter__()
        __next__()
```

### Settings options

```
pool_factory ("auto") - Redis **kwargs
    "auto" / "cluster" - Try to figure out automatically what the redis server type is (currently cluster / no-cluster)
    "pool" - Force non cluster aware connection pool
address (None) - Redis **kwargs
    An (address, port) tuple for tcp sockets. The default is (localhost, 6379)
    An string if it's a path for unix domain sockets
addresses (None) - Redis **kwargs
    Multiple (address, port) tuples for cluster ips for fallback. The default is ((localhost, 6379), )
username (None) - Redis **kwargs
    If you have an ACL username, specify it here (expects utf-8 encoding if string)
password (None) - Redis **kwargs
    If you have an AUTH / ACL password, specify it here (expects utf-8 encoding if string)
client_name (None) - Redis **kwargs
    If you want your client to be named on connection specify it here (expects utf-8 encoding if string)
resp_version (-1) - Redis **kwargs
    Specifies which RESP protocol version to use for connections, -1 = auto detect, else 2 or 3
socket_factory ("tcp") - Redis **kwargs
    Specifies which socket type to use
connect_retry (2) - Redis **kwargs
    How many attempts to retry connecting when establishing a new connection
max_connections (None) - Redis **kwargs
    How many maximum concurrent connections to keep to the server in the connection pool. The default is unlimited
wait_timeout (None) - Redis **kwargs
    How long (float seconds) to wait for a connection when the connection pool is full before returning an timeout error. The default is unlimited
cutoff_size (6000) - Redis **kwargs
    The maximum ammount of bytes that will be tried to be appended before sending data to the socket
decoder (None)
encoder (None)
with_attributes (False)
database (None)
endpoint (False)
connect_timeout (None)
socket_timeout (None)
tcp_keepalive (None)
tcp_nodelay (True)
ssl_context (None)
```

### Exceptions

## Redis command replacements

The following redis commands should not be called directly, but via the library API:

### Username and Password (AUTH / ACL)

If you have a username or/and password you want to use, pass them to the connection constructor, such as:

```python
r = Redis(username="your_username", password="your_password")
```

### Database selection (SELECT)

You can specify the default database you want to use at the constructor:

```python
r = Redis(database=1)
```

If you want to modify it afterwards, you can use a modify context for it:

```python
with r.modify(database=2) as r1:
    r1("set", "a", "b")
```

### Transaction (WATCH / MULTI / EXEC / DISCARD)

To use the transaction commands, you must take a connection, and use all the commands inside. Please read the Connection section below for more details.

### Pubsub and monitor (SUBSCRIBE / PSUBSCRIBE / UNSUBSCRIBE / PUNSUBSCRIBE / MONITOR)

To use the transaction commands, you must take a push connection, and use all the commands inside. Please read the Connection section below for more details.

## Additional commands

### Cluster commands

Currently the library supports talking to redis master servers only. It knows automatically when you are connected to a cluster.

If you want to specify multiple addresses for redundency, you can do so:

```python
r = Redis(addresses=(('host1', port1), ('host2', port2)))
```

You can get list of servers with the endpoints() method.

You can also send a command to all the masters by adding ```endpoint='masters'``` to the call:

```python
r("cluster", "info", endpoint="masters")
```

You can also open a connection (for example to get key space notifications or monitor to a specific instance by adding ```endpoint=<the server address>```) to the connection() method.

### Connection commands

The connection() method is required to be used for sending multiple commands to the same server or to talk to the server in push mode (pub/sub & monitor).

You can pass to the method push=True for push mode (else it defaults to a normal connection).

While you do not have to pass a key, it's better to provide one you are about to use inside, in case you want to use a cluster later on.
