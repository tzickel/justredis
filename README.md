What ?
---
A redis client for Python

Please note that this project is currently alpha quality and the API is not finalized. Please provide feedback if you think the API is convenient enough or not. A permissive license will be chosen once the API will be more mature for wide spread consumption.

Why ?
---

RESP2/RESP3 support

The client can talk to Redis servers both as RESP2 and RESP3. You can explicitly specify which version you want, and specify if you are intrested in recving attributes or not (this effects which result types you get). The API Questions below documents what this means.

Cluster support

The client will automatically detect if the server is part of a cluster or not, and figure out which keys are part of a command. You can manually force it to not use a cluster.

Per command optional settings (encoding, decoding, attribues)

If you configured the client to decode the results as unicode strings, but you want a specific result to be left as an bytes, you can specify that. The API Questions below shows how to do that.

If for set the connection to handle results as unicode strings, but you want to recive a specific 


Roadmap
---
Async support with the same exact API (but with the await keyword), currently for asyncio
Transparent EVAL script caching
More cluster features (both master, and replicas)
Clear seperation between the I/O and concurrency code to the rest

Should allow for easy portablity to other event loops such as gevent, and async ones such as asyncio and trio.

Not on roadmap (for now)
---
hiredis support
sentinal server support
high level features which are not part of the redis specification (such as locks)
manually specifing each command it's result and help (maybe for special stuff like bit operations?)

Installing
---

Examples
---

API
---

Redis command replacments
---

API questions
---
