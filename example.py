from justredis import SyncRedis
import time


#with SyncRedis(resp_version=2) as r:
#    print(r('get', 'a'))

#r = SyncRedis(resp_version=2, socket_factory='unix')
r = SyncRedis(resp_version=2, encoder='ascii')

print(r('set', 'a', 'bשלום', encoder='utf8'))
with r.database(1) as r1:
    print(r1('get', 'a'))
print(r('get', 'a'))

#with r.monitor() as m:
    #for item in m:
        #print(item)

#s = time.time()
#for _ in range(100):
#    r('set', 'a', 'b' * 100000)
#    assert len(r('get', 'a')) == 100000
#e = time.time()
#print(e-s)
#r.close()
#print(r('get', 'a'))
#print(r('set', 'a', 'c'))
#print(r(('get', 'a'), ('set', 'a', 'c')))

#from connection import Connection


#c = Connection(('localhost', 6379), resp_version=2, timeout=None)

#print(c('get', 'a'))

#print(c.command('get', 'a'))


#c = SyncConnection(('localhost', 6379), resp_version=2, timeout=None, username='blah', password='yey')
#c = SyncConnection(('localhost', 6379), resp_version=3, timeout=None)
#c = SyncConnection(('localhost', 6379), timeout=None)
#print(c.c('del', 'a', 'b').data)
#print(c.command('SUBSCRIBE', 'a'))
#print(c.command('CLIENT', 'TRACKING', 'ON'))
#print(c.command(''))
#print(c.command('SET', 'a', 'b'))
#print(c.command('MGET', 'a', 'b'))
#print(c.command('HELLO', 3))
#print(c.commands(('SET', 'a', 'b'), ('GET', 'a')))
#try:
    #print(c.command('BLPOP', 'aaa', 3))
#except:
    #pass
#print()

#c.push_command('subscribe', 'blah')
#print(c.pushed_message())
#while True:
#    print(c.pushed_message(1))
#    print(c.command('GET', 'a'))
#print(c.command('SUBSCRIBE', 'BLAH'))
