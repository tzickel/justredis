import os
import random
import signal
import subprocess
import sys


platform = ''
if sys.platform.startswith('linux'):
    platform = 'linux'
elif sys.platform.startswith('darwin'):
    platform = 'darwin'
elif sys.platform.startswith('win'):
    platform = 'windows'


# TODO (misc) make this a contextmanager to cleanup properly on failures
def start_cluster(masters, dockerimage=None, extraparams='', ipv4=True):
    addr = '127.0.0.1' if ipv4 else '::1'
    ret = []
    if dockerimage is None:
        subprocess.call('rm /tmp/justredis_cluster*.conf', shell=True)
    for x in range(masters):
        ret.append(RedisServer(dockerimage=dockerimage, extraparams='--cluster-enabled yes --cluster-config-file /tmp/justredis_cluster%d.conf' % x))
    # Requires redis-cli from version 5 for cluster management support
    if dockerimage:
        stdout = subprocess.Popen('docker run -i --rm --net=host ' + dockerimage +' redis-cli --cluster create ' + ' '.join(['%s:%d' % (addr, server.port) for server in ret]), stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True).communicate(b'yes\n')
    else:
        stdout = subprocess.Popen('redis-cli --cluster create ' + ' '.join(['%s:%d' % (addr, server.port) for server in ret]), stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True).communicate(b'yes\n')
    if stdout[0] == b'':
        raise Exception('Empty output from redis-cli ?')
    return ret, stdout[0]


class RedisServer(object):
    def __init__(self, dockerimage=None, extraparams=''):
        self._proc = None
        while True:
            self.close()
            self._port = random.randint(1025, 65535)
            if dockerimage:
                #cmd = 'docker run --rm -p {port}:6379 {image} --save {extraparams}'.format(port=self.port, image=dockerimage, extraparams=extraparams)
                cmd = 'docker run --rm --net=host {image} --save --port {port} {extraparams}'.format(port=self.port, image=dockerimage, extraparams=extraparams)
            else:
                cmd = 'redis-server --save --port {port} {extraparams}'.format(port=self.port, extraparams=extraparams)
            kwargs = {}
            if platform == 'linux':
                kwargs['preexec_fn'] = os.setsid
            self._proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, **kwargs)
            seen_redis = False
            while True:
                line = self._proc.stdout.readline()
                if b'Redis' in line:
                    seen_redis = True
                if line == b'':
                    self._port = None
                    break
                elif b'Ready to accept connections' in line:
                    break
                elif b'Opening Unix socket' in line and b'Address already in use' in line:
                    raise Exception('Unix domain already in use')
            # usually could not find docker image
            if not seen_redis:
                raise Exception('Could not run redis')
            if self._port:
                break

    @property
    def port(self):
        return self._port

    def close(self):
        if self._proc:
            try:
                self._proc.stdout.close()
                if platform == 'linux':
                    os.killpg(os.getpgid(self._proc.pid), signal.SIGTERM) 
                else:
                    self._proc.kill()
                self._proc.wait()
            except Exception:
                pass
            self._proc = None

    def __del__(self):
        self.close()
