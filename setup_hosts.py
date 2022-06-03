import os
import shlex
import subprocess
from concurrent.futures import ThreadPoolExecutor, wait

from paramiko import SSHClient

DASK_HOST = '192.168.2.1'
SCHEDULER_PORT = 8001


def run(cmd, **kwargs):
    if isinstance(cmd, str):
        cmd = shlex.split(cmd)
    print(f"> {' '.join(cmd)}")  # type:ignore
    subprocess.run(cmd, **kwargs)


def start_host(host):
    print('Starting host:', host)
    PASSWORD = os.environ['RPI_PASSWORD']
    client = SSHClient()
    client.load_system_host_keys()
    client.connect(host, username='silvester', password=PASSWORD)
    # Kill any running mongodb and dask-workers
    client.exec_command('pkill -9 -f "dask-worker"')
    client.exec_command('pkill -9 mongod')
    client.exec_command('sleep 3')
    # Start mongodb and dask-worker for the scheduler
    client.exec_command('mkdir -p ./data')
    client.exec_command(f'nohup mongod --replSet "rs0" --bind_ip {host} --dbpath ./data &')
    client.exec_command(f'nohup dask-worker tcp://{DASK_HOST}:{SCHEDULER_PORT} &')
    client.close()


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
