## What ?
A redis client for Python

Please note that this project is currently alpha quality and the API is not finalized. Please provide feedback if you think the API is convenient enough or not. A permissive license will be chosen once the API will be more mature for wide spread consumption.

## Why ?

- RESP2/RESP3 support
- Transparent API (automatic figure out redis features)
- Cluster support
- Per context / command properties (database #, decoding, encoding, RESP3 attributes, etc...)
- Pipelining
- Modular API allowing for support for multiple synchronous and (later) asynchronous event loops

## Roadmap

- Asynchronous I/O support with the same exact API (but with the await keyword)
- Transparent EVAL script caching
- Redis client-side caching

## Not on roadmap (for now?)

- Hiredis support
- Sentinal support
- High level features which are not part of the redis specification (such as locks, retry watchs, etc...)
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

TBD

## Redis command replacements
The following redis commands should not be called directly, but via the library API:

### Password (AUTH / ACL)

### Database selection (SELECT)

### Transaction (WATCH / MULTI / EXEC / DISCARD)

### Pubsub and monitor (SUBSCRIBE / PSUBSCRIBE / UNSUBSCRIBE / PUNSUBSCRIBE / MONITOR)

## Additional commands

### Cluster commands

TBD

## Open questions

TBD
