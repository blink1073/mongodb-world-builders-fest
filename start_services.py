import atexit
import os
import shlex
import subprocess
import sys
import time
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

# Set up the hosts.
run([sys.executable, 'setup_hosts.py'], check=True)
with open('host_list.txt') as fid:
    hosts = fid.readlines()

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

