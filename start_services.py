import atexit
import os
import shlex
import subprocess
import time
import xml.etree.ElementTree as ET

from paramiko import SSHClient
from pymongo.mongo_client import MongoClient
from pymongo.errors import OperationFailure


DASK_HOST = 'localhost'  # '192.168.2.1'


def run(cmd, **kwargs):
    if isinstance(cmd, str):
        cmd = shlex.split(cmd)
    print(f"> {' '.join(cmd)}")  # type:ignore
    subprocess.run(cmd, **kwargs)

# Kill any local mongodb and dask-schedulers
run('pkill -9 -f "dask-scheduler"')
run('pkill -9 mongod')
time.sleep(3)

# Start a local dask-scheduler.
SCHEDULER_PORT = 8001
dask_proc = subprocess.Popen(['dask-scheduler', '--host', DASK_HOST, '--port', str(SCHEDULER_PORT)])
atexit.register(dask_proc.kill)

# Find the remote hosts using nmap
# Bridge is created using System Preferences > Sharing > Internet Sharing
# with the ethernet adapter port.
#run('nmap -T4 -v -sn 192.168.2.0/24 -oX report.xml', check=True)

# Parse the report.xml to get the available hosts.
hosts = []
mytree = ET.parse('report.xml')
for host in mytree.findall('host'):
    if host.find('status').get('state') != 'down':
        hosts.append(host.find('address').get('addr'))

with open('host_list.txt', 'w') as fid:
    fid.writelines(hosts)

# Set up each host.
for host in hosts:
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

# Start the local mongodb.
os.makedirs('./data', exist_ok=True)
cmd = 'mongod --replSet "rs0" --port 27017 --dbpath ./data'
mongo_proc = subprocess.Popen(shlex.split(cmd))
atexit.register(mongo_proc.kill)

# Start the replicaset.
init_doc = dict(_id="rs0", members=[dict(_id=0, host="localhost:27017")])
for (i, host) in enumerate(hosts):
    init_doc["members"].append(dict(_id=i + 1, host=host))  # type:ignore

con = MongoClient(directConnection=True)
try:
    con['admin'].command({'replSetGetStatus': 1})
except OperationFailure:
    # not initiated yet
    for i in range(30):
        try:
            con['admin'].command({'replSetInitiate': init_doc})
            break
        except OperationFailure as e:
            print(e.message + " - will retry")  # type:ignore
            time.sleep(1)


con = MongoClient()
con.admin.command('ping')
print('\n\nStarted Mongo Server:')
print(con.topology_description)
print('\n\n')


while 1:
    try:
        time.sleep(1)
    except KeyboardInterrupt:
        print('Closing Sessions')
        break

