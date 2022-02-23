## What ?

A Redis client for Python supporting many Redis features and Python synchronous (Python 3.5+) and asynchronous (Python 3.6+) communication.

## [Why](https://xkcd.com/927/) ?

- Transparent API (Just call the Redis commands, and the library will figure out cluster routing, script caching, etc...)
- Per context and command properties (database #, decoding, RESP3 attributes)
- Asynchronous I/O support with the same exact API (but with the await keyword), targeting asyncio and trio (using [AnyIO](https://github.com/agronholm/anyio) which needs to be installed as well if you want async I/O)
- Modular API allowing for easy support for multiple synchronous and asynchronous event loops and disabling of unneeded features
- CI Testing for CPython 3.5, 3.6, 3.7, 3.8, 3.9 and PyPy3 with Redis 5 and Redis 6
- No legacy support for old language features

## Redis features support table

| Feature | Supported | Notes |
| --- | --- | --- |
| [Transactions](https://redis.io/topics/transactions) | V | See [examples](#examples) and [Transaction section](#redis-command-replacements) |
| [Pub/Sub](https://redis.io/topics/pubsub) | V | See [examples](#examples) and [Pub/Sub and monitor section](#redis-command-replacements) |
| [Pipelining](https://redis.io/topics/pipelining) | V | See [examples](#examples) and [Pipelining section](#pipelining) |
| [Cluster](https://redis.io/topics/cluster-spec) | V | See [Cluster commands](#cluster-commands) |
| [RESP3 support](https://github.com/antirez/RESP3/blob/master/spec.md) | V | See [RESP2 and RESP3 difference section](#resp2-and-resp3-difference) |
| [SSL](https://redis.io/topics/encryption) | V | See the [SSL connection parameters](#settings-options) |
| [Script caching](https://redis.io/commands/evalsha) | X |  |
| [Client side caching](https://redis.io/topics/client-side-caching) | X |  |
| [Sentinel](https://redis.io/topics/sentinel) | X |  |

## Roadmap

Getting it out of alpha:
- [ ] API Finalization (your feedback is appreciated)
  - [ ] Should calling redis be using \_\_call__() or a special method such as "command" for refactoring ?
  - [ ] Is the modify() API flexiable enough ?
  - [ ] Is it easy to extend the module ?
  - [ ] Should we add helper functions for SCAN iterators and other API ?
- [ ] Resolving some of the TODO in code

General:
- More features in the support table
- Better test coverage
- Resolve all TODO in code
- Move documentation to topics + docstrings
- Automate code convertion between sync and async
- More CI checks such as flake8, pylint, etc..

## Not on roadmap (for now?)

- High level features which are not part of the Redis specification (such as locks, retry transactions, etc...)
- Manual command interface (maybe for special stuff like bit operations ?)
- Python 2 support (can be added, but after removing some misc syntax features)

## Installing

The project can be found in PyPI as justredis. Install it via pip or requierments.txt file:

```bash
pip install justredis
```

If you want to use asynchronous I/O frameworks asyncio or trio or curio with this library, you need to install the AnyIO library as well:

```bash
pip install anyio
```

## Examples

```python
from justredis import Redis


# Let's connect to localhost:6379 and decode the string results as utf-8 strings
r = Redis(decoder="utf8")
assert r("set", "a", "b") == "OK"
assert r("get", "a") == "b"
assert r("get", "a", decoder=None) == b"b" # But this can be changed per command


# We can even run commands on a different database number
with r.modify(database=1) as r1:
    assert r1("get", "a") == None # In this database, a was not set to b


# Here we can use a transactional set of commands
# Notice that when we take a connection, if we plan on cluster support, we need
# to tell it a key we plan on using inside, or a specific server address
with r.connection(key="a") as c:
    c("multi")
    c("set", "a", "b")
    c("get", "a")
    assert c("exec") == ["OK", "b"]


# Or we can just pipeline the commands from before
with r.connection(key="a") as c:
    result = c(("multi", ), ("set", "a", "b"), ("get", "a"), ("exec", ))[-1]
    assert result == ["OK", "b"]


# Here is the famous increment example
# Notice we take the connection inside the loop,
# this is to make sure if the cluster moved the keys, it will still be ok
while True:
    with r.connection(key="counter") as c:
        c("watch", "counter")
        value = int(c("get", "counter") or 0)
        c("multi")
        c("set", "counter", value + 1)
        if c("exec") is None: # Redis returns None in EXEC command when the transaction failed
            continue
        value += 1 # The value is updated if we got here
        break


# Let's show some publish and subscribe commands,
# here we use a push connection (where commands have no direct response)
with r.connection(push=True) as p:
    p("subscribe", "hello")
    assert p.next_message() == ["subscribe", "hello", 1]
    assert p.next_message(timeout=0.1) == None # Let's wait 0.1 seconds for another result
    r("publish", "hello", ", World !")
    assert p.next_message() == ["message", "hello", ", World !"]
```

## API

```python
Redis(**kwargs)
    @classmethod
    from_url(url, **kwargs)
    __enter__() / __exit__()
    close()
    # kwargs options = endpoint, decoder, attributes, database
    __call__(*cmd, **kwargs)
    endpoints()
    # kwargs options = decoder, attributes, database
    modify(**kwargs) # Returns a modified settings instance (while sharing the pool)
    # kwargs options = key, endpoint, decoder, attributes, database
    connection(push=False, **kwargs)
        __enter__() / __exit__()
        close()
        # kwargs options = decoder, attributes, database
        __call__(*cmd, **kwargs) # On push connection no result for calls
        # kwargs options = decoder, attributes, database
        modify(**kwargs) # Returns a modified settings instance (while sharing the connection)

        # Push connection only commands
        # kwargs options = decoder, attributes
        next_message(timeout=None, **kwargs)
        __iter__()
        __next__()
```

### URI connection options

```Redis.from_url()``` options are:

Regular TCP connection
```
redis://[[username:]password@]host[:port][/database][[?option1=value1][&option2=value2]]
```

SSL TCP connection (you can use ssl instead of rediss)
```
rediss://[[username:]password@]host[:port][/database][[?option1=value1][&option2=value2]]
```

Unix domain connection (you can use unix instead of redis-socket)
```
redis-socket://[[username:]password@]path][[?option1=value1][&option2=value2]]
```

For cluster, you can replace host:port with a list of host1:port1,host2:port2,... if you want fallback options for backup.

You can add options in the end from the Redis constructor options below.

### Settings options

This are the ```Redis()``` constructor options:
```
pool_factory ("auto")
    "auto" / "cluster" - Try to figure out automatically what the Redis server type is (currently cluster / no-cluster)
    "pool" - Force non cluster aware connection pool (simpler code)
address (None)
    An (address, port) tuple for tcp sockets, the default is (localhost, 6379)
    An string if it's a path for unix domain sockets, the default is "/tmp/redis.sock"
username (None)
    If you have an ACL username, specify it here
password (None)
    If you have an AUTH / ACL password, specify it here
client_name (None)
    If you want your client to be named on connection specify it here
resp_version (2)
    Specifies which RESP protocol version to use for connections
    -1 = auto detect
    2 = RESP2
    3 = RESP3
socket_factory ("tcp")
    Specifies which socket type to use to connect to the redis server
    "tcp" tcp socket
    "unix" unix domain socket
    "ssl" tcp ssl socket
connect_retry (2)
    How many attempts to retry connecting when establishing a new connection
max_connections (None)
    How many maximum concurrent connections to keep to a server in the connection pool, the default is unlimited
wait_timeout (None)
    How long (float seconds) to wait for a connection when the connection pool is full before returning an timeout error, the default is unlimited
cutoff_size (6000)
    The maximum ammount of bytes that will be appended together instead of sent seperatly before sending data to the socket, 0 to disable this feature
custom_command_class (None)
    Register a custom class to extend redis server commands handling
encoder (None)
    Specify how to encode strings to bytes, it can be a string, list or dictionary that are passed directly as the parameters to str.encode, the default is "utf8"
connect_timeout (None)
    How many (float seconds) to wait for a connection with a server to be established, the default is unlimited
socket_timeout (None)
    How many (float seconds) to wait for a socket operation (read/write) with a server, the default is unlimited    
```

This parameters can be passed to the ```Redis()``` constructor, or to the ```modify()``` method or per ```__call__()```:
```
decoder (None)
    Specify how to decode the string results from the server, it can be a string, list or dictionary that are passed directly as parameters to bytes.decode, the default is normal bytes conversion
attributes (False)
    Specify if you want to handle the attributes fields from the RESP3 protocol (read the special section about this in the readme)
database (None)
    Set which database to operate on the server, the default is 0
```

This can be provided to the ```Redis()``` constructor if you are using the cluster pool_factory:
```
addresses (None)
    Multiple (address, port) tuples for cluster ips for fallback. The default is ((localhost, 6379), )
```

This can be provided to the ```Redis()``` constructor for tcp and ssl socket_factory:
```
tcp_keepalive (None)
    How many seconds to check the TCP connection liveness, the default is disabled
tcp_nodelay (True)
    Enable or disable the TCP nodelay algorithm
```

This can be provided to the ```Redis()``` constructor for ssl socket_factory:
```
ssl_context (None)
    An Python SSL context object, the default is Python's ssl.create_default_context()
ssl_cafile (None)
    A path to the CA certificate file on disk, works only if ssl_context is None
ssl_certfile (None)
    A path to the server certificate file on disk, works only if ssl_context is None
ssl_keyfile (None)
    A path to the server key file on disk, works only if ssl_context is None
```

Read the cluster and connection documentation below for the options for the ```connection()``` and ```__call__()``` API

### Exceptions

```
ValueError - Will be thrown when an invalid input was given. Nothing will be sent to the server.
Error - Will be thrown when the server returned an error to a request.
PipelinedExceptions - Will be thrown when some of the pipeline failed.
RedisError - Will be thrown when an internal logic error has happened.
    CommunicationError - An I/O error has occured.
    ConnectionPoolError - The connection pool could not get a new connection.
    ProtocolError - Invalid input from the server.
```

## Redis command replacements

The following Redis commands should not be called directly, but via the library API:

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

If you want to modify it afterwards for a specific set of commands, you can use a modify context for it:

```python
with r.modify(database=2) as r1:
    r1("set", "a", "b")
```

### Transaction (WATCH / MULTI / EXEC / DISCARD)

To use the transaction commands, you must take a connection, and use all the commands inside. Please read the [Connection section](#connection-commands) below for more details.

### Pub/Sub and monitor (SUBSCRIBE / PSUBSCRIBE / UNSUBSCRIBE / PUNSUBSCRIBE / MONITOR)

To use push commands, you must take a push connection, and use all the commands inside. Please read the [Connection section](#connection-commands) below for more details.

## Usage

### Connection commands

The ```connection()``` method is required to be used for sending multiple commands to the same server (such as transactions) or to talk to the server in push mode (pub/sub and monitor).

You can pass to the method ```push=True``` for push mode where commands have no direct response (else it defaults to a normal connection).

While you do not have to pass a ```key=```, it's better to provide one you are about to use inside, in case you want to talk to a cluster later on.

There are some instances you might want to talk to a specific server in a cluster (like getting keyspace notifications from it), so you can pass ```endpoint=``` instead of ```key=``` with that server's address.

Check the [transaction or pubsub examples](#examples) above for syntax usage.

### Pipelining

You can pipeline multiple commands together by passing an list of commands to be sent together. This is usually to have better latency.

Notice that if you are talking to a cluster, the pipeline must contain commands which handle keys in the same keyslots of a given server.

If some of the commands failed, an PipelinedExceptions exception will be thrown, with it's args pointing to the result of each command.

Check the [pipeline example](#examples) above for syntax usage.

### Cluster commands

Currently the library supports talking to Redis master servers only. It knows automatically when you are connected to a cluster (unless you disabled that feature in the constructor settings explicitly).

If you want to specify multiple addresses for redundency, you can do so:

```python
r = Redis(addresses=(('host1', port1), ('host2', port2)))
```

You can get the list of servers with the ```endpoints()``` method.

You can also send a command to all the masters by adding ```endpoint='masters'``` to the ```__call__()```:

```python
r("cluster", "info", endpoint="masters")
```

You can also open a connection to a specific instance, for example to get key space notifications or monitor it by adding ```endpoint=<the server address>``` to the ```connection()``` method).

### RESP2 and RESP3 difference

The library supports talking both in RESP2 and RESP3. By default it will use RESP2, because this way you'll get same response whether you are talking to a RESP3 supporting server (Redis server version 6 and above) or not.

You can still tell it to use RESP3 or to auto negotiate the highest version with the specific server:

```python
r = Redis(resp_version=2) # Talk RESP2 only
r = Redis(resp_version=3) # Talk RESP3 only (will throw an Exception if server does not support it)
r = Redis(resp_version=-1) # Talk in the highest version possible
```

You can read about RESP3 protocol and responses in the [Redis documentation](https://github.com/antirez/RESP3/blob/master/spec.md).

RESP3 allows the clients to know the response type (such as strings, lists, dictionaries, sets...), and Justredis supports all of the response types.

RESP3 provides an option to get with the results extra attributes. Since Python's type system cannot add the attributes easily, another configuration value was added, ```attributes``` which specifies if you care about getting this information or not, the default is False:

```python
r = Redis(attributes=True)
```

If attributes is disabled, you will get the direct Python mapping of the results (set, list, dict, string, numbers, etc...) and if enabled, you will get a special object which will hold the raw data in the ```data``` attribute, and the attributes in the ```attrs``` attribute. Notice that this feature is orthogonal to choosing RESP2 / RESP3 (but in RESP2 the attrs will always be empty), for ease of development.

Here is an example of the difference in Redis version 6, with and without attributes:
```python
>>> import justredis
>>> r = justredis.Redis() # By default it connects via RESP2
>>> r("hgetall", "aaa")
[b'bbb', b'ccc', b'ccc', b'ddd']
>>> r("hgetall", "aaa", attributes=True) # This is RESP2 with attributes, it has .data and .attrs
Array: [String: b'bbb' , String: b'ccc' , String: b'ccc' , String: b'ddd' ] 
>>> r = justredis.Redis(resp_version=-1) # This will connect to Redis 6 with RESP3
>>> r("hgetall", "aaa")
OrderedDict([(b'bbb', b'ccc'), (b'ccc', b'ddd')]) # This is Python's OrderedDict
>>> r("hgetall", "aaa", attributes=True)
Map: OrderedDict([(String: b'bbb' , String: b'ccc' ), (String: b'ccc' , String: b'ddd' )])
```

### Thread and async safety

The library is thread safe and async safe. Do not pass Connection objects between different threads or coroutines.

### Modify

You can change some of the settings on a per ```__call()__``` call, or if you want multiple calls to have different settings, you can use the ```modify()``` method on a Connection or Redis objects.

Currently you can change the string decoder used, the database number and the attributes flag. Check the [examples](#examples) above to see how it's done.

### Serialization and deserialization

The library supports as inputs only this types: bytes, bytearray, memoryview, str, int and float. If you pass a string, it will be encoded to bytes by the given encoder option (default is utf-8). Passing anything else will result in a ValueError.

The library will return the data types that the RESP protocol returns as described in the RESP section. Exceptions will always be utf-8 string encoded and for other string results, you can decide to keep them as bytes, or to decode them to a string.

### Async support

The API for the asynchronous commands is exactly the same, just adding "await" where it's needed:

```python
AsyncRedis(**kwargs)
    @classmethod
    from_url(url, **kwargs)
    async __aenter__() / __aexit__()
    async aclose()
    # kwargs options = endpoint, decoder, attributes, database
    async __call__(*cmd, **kwargs)
    async endpoints()
    # kwargs options = decoder, attributes, database
    modify(**kwargs) # Returns a modified settings instance (while sharing the pool)
    # kwargs options = key, endpoint, decoder, attributes, database
    async connection(push=False, **kwargs)
        async __aenter__() / async __aexit__()
        async aclose()
        # kwargs options = decoder, attributes, database
        async __call__(*cmd, **kwargs) # On push connection no result for calls
        # kwargs options = decoder, attributes, database
        modify(**kwargs) # Returns a modified settings instance (while sharing the connection)

        # Push connection only commands
        # kwargs options = decoder, attributes
        async next_message(timeout=None, **kwargs)
        __iter__()
        async __next__()
```

Don't forget there is no ```__del__()``` method in async code, so call ```aclose()``` or use async context managers when needed.

### Extending the library with more command support

You can extend the Redis object to support real redis commands, and not just calling them raw, here is an example:

```python
from justredis import Redis


class CustomCommands:
    def __init__(self, base):
        self._base = base

    def get(self, key, **kwargs):
        return self._base("get", key, **kwargs)

    def set(self, key, value, **kwargs):
        return self._base("set", key, value, **kwargs)


r = Redis(custom_command_class=CustomCommands)
r.set("hi", "there")
assert r.get("hi", decoder="utf8") == "hi"
with r.modify(database=1) as r1:
    assert r1.get("hi") == None
```
