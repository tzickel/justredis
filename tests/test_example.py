from justredis import Redis, Error


def example():
    # Let's connect to localhost:6379 and decode the string results as utf-8 strings.
    r = Redis(decoder="utf8")
    assert r("set", "a", "b") == "OK"
    assert r("get", "a") == "b"
    assert r("get", "a", decoder=None) == b"b"  # But this can be changed on the fly

    with r.modify(database=1) as r1:
        assert r1("get", "a") == None  # In this database, a was not set to b

    # Here we can use a transactional set of commands
    with r.connection(key="a") as c:  # Notice we pass here a key from below (not a must if you never plan on connecting to a cluster)
        c("multi")
        c("set", "a", "b")
        c("get", "a")
        assert c("exec") == ["OK", "b"]

    # Or we can just pipeline them.
    with r.connection(key="a") as c:
        result = c(("multi",), ("set", "a", "b"), ("get", "a"), ("exec",))[-1]
        assert result == ["OK", "b"]

    # Here is the famous increment example
    # Notice we take the connection inside the loop, this is to make sure if the cluster moved the keys, it will still be ok.
    while True:
        with r.connection(key="counter") as c:
            c("watch", "counter")
            value = int(c("get", "counter") or 0)
            c("multi")
            c("set", "counter", value + 1)
            if c("exec") is None:
                continue
            value += 1  # The value is updated if we got here
            break

    # Let's show some publish & subscribe commands, here we use a push connection (where commands have no direct response)
    with r.connection(push=True) as p:
        p("subscribe", "hello")
        assert p.next_message() == ["subscribe", "hello", 1]
        assert p.next_message(timeout=0.1) == None  # Let's wait 0.1 seconds for another result
        r("publish", "hello", ", World !")
        assert p.next_message() == ["message", "hello", ", World !"]


if __name__ == "__main__":
    example()
