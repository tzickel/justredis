from justredis import Redis

r = Redis(resp_version=2)
print(r('get', 'a'))
print(r('set', 'a', 'c'))

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
