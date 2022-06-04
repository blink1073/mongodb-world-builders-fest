import os
import shlex
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, wait

from paramiko import SSHClient

PARENT_HOST = '192.168.2.1'
SCHEDULER_PORT = 8001


def run(cmd, **kwargs):
    if isinstance(cmd, str):
        cmd = shlex.split(cmd)
    print(f"> {' '.join(cmd)}")  # type:ignore
    subprocess.run(cmd, **kwargs)


def execute(ssh, cmd):
    _, stdout, stderr = ssh.exec_command(cmd, get_pty=True)
    for line in iter(stdout.readline, ""):
        print(line, end="")
    for line in iter(stderr.readline, ""):
        print(line, end="")

def execute_chan(chan, cmd):
    buff = ''
    while not buff.strip().endswith('$'):
        resp = ''
        resp = chan.recv(9999).decode('utf-8')
        print(resp)
        buff += resp

    chan.send(cmd + '\n')

    buff = ''
    while not buff.strip().endswith('$'):
        resp = ''
        resp = chan.recv(9999).decode('utf-8')
        print(resp)
        buff += resp


def start_host(host):
    print('Starting host:', host)
    PASSWORD = os.environ['RPI_PASSWORD']
    client = SSHClient()
    client.load_system_host_keys()
    client.connect(host, username='silvester', password=PASSWORD)

    # Set up tasks.
    execute(client, 'uname -a')
    execute(client, 'pkill -15 -f "dask-worker"')
    execute(client, 'pkill -15 mongod')
    execute(client, 'rm -rf ./data')
    execute(client, 'mkdir ./data')
    time.sleep(3)

    # Start mongodb and dask-worker for the scheduler.
    execute(client, f'mongod --fork --logpath ./data/mongod.log --replSet "rs0" --bind_ip {host} --dbpath ./data')

    channel = client.invoke_shell('bash')
    execute_chan(channel, f'PATH="/home/silvester/miniforge3/bin:$PATH"; nohup dask-worker tcp://{PARENT_HOST}:{SCHEDULER_PORT} &')
    time.sleep(2)
    channel.close()

    client.close()
    print('Finished host:', host)


pool = ThreadPoolExecutor(1)
hosts = []
futures = []

# Check ports 2-254.
for i in range(2, 255):
    host = f'192.168.2.{i}'
    try:
        run(f'ping {host} -o -t 1', check=True)
        hosts.append(host)
        futures.append(pool.submit(start_host, host))
    except Exception:
        pass

with open('host_list.txt', 'w') as fid:
    fid.writelines(hosts)

wait(futures)
