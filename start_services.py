import atexit
import os
import shlex
import shutil
import subprocess
import sys
import time
from pymongo.mongo_client import MongoClient
from pymongo.errors import OperationFailure


HOST = '192.168.2.1'
MONGO = '/Users/steve.silvester/.local/m/versions/4.4.0/bin/mongod'


def run(cmd, **kwargs):
    if isinstance(cmd, str):
        cmd = shlex.split(cmd)
    print(f"> {' '.join(cmd)}")  # type:ignore
    subprocess.run(cmd, **kwargs)

# Kill any local mongodb and dask-schedulers
run('pkill -15 -f "dask-scheduler"')
run('pkill -15 mongod')
time.sleep(3)

# Start a local dask-scheduler.
SCHEDULER_PORT = 8001
dask_proc = subprocess.Popen(['dask-scheduler', '--host', HOST, '--port', str(SCHEDULER_PORT)])
atexit.register(dask_proc.kill)

# Set up the hosts.
run([sys.executable, 'setup_hosts.py'], check=True)
with open('host_list.txt') as fid:
    hosts = [host.strip() for host in fid.readlines()]

# Start the local mongodb.
if os.path.exists('./data'):
    shutil.rmtree('./data')
os.makedirs('./data')
cmd = f'{MONGO} --fork --logpath ./data/mongod.log --replSet "rs0" --bind_ip {HOST} --port 27017 --dbpath ./data'
mongo_proc = subprocess.Popen(shlex.split(cmd))
atexit.register(mongo_proc.kill)

# Wait for replicaset to start up.
print('\nWaiting for replicaset')
con = MongoClient(f"{HOST}:27017", directConnection=True)
for i in range(30):
    try:
        resp = con['admin'].command({'replSetGetStatus': 1})
        assert resp["ok"]
        print('\nReplicaset was initiated!')
        break
    except OperationFailure as e:
        print(str(e) + " - will retry")  # type:ignore
        time.sleep(1)
con.close()

con = MongoClient(f"{HOST}:27017", replicaset="rs0")
con.admin.command('ping')
print('\n\nStarted Mongo Replicaset:')
print(con.topology_description)
print('\n\n')
con.close()

while 1:
    try:
        time.sleep(1)
    except KeyboardInterrupt:
        print('Closing Sessions')
        break

