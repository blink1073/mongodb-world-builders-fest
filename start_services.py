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
MONGO = os.environ['MONGO_BINARY']


def run(cmd, **kwargs):
    if isinstance(cmd, str):
        cmd = shlex.split(cmd)
    print(f"> {' '.join(cmd)}")  # type:ignore
    subprocess.run(cmd, **kwargs)

# Kill any local mongodb and dask-schedulers
run('pkill -15 -f "dask-scheduler"')
run('pkill -15 mongod')
time.sleep(3)

# Start the local mongodb.
if os.path.exists('./data'):
    shutil.rmtree('./data')
os.makedirs('./data')
cmd = f'{MONGO} --fork --logpath ./data/mongod.log --replSet "rs0" --bind_ip {HOST} --port 27017 --dbpath ./data'
mongo_proc = subprocess.Popen(shlex.split(cmd))
atexit.register(mongo_proc.kill)

# Start a local dask-scheduler.
SCHEDULER_PORT = 8001
dask_proc = subprocess.Popen(['dask-scheduler', '--host', HOST, '--port', str(SCHEDULER_PORT)])
atexit.register(dask_proc.kill)

# Set up the hosts.
run([sys.executable, 'setup_hosts.py'], check=True)
with open('host_list.txt') as fid:
    hosts = [host.strip() for host in fid.readlines()]

# Wait for replicaset to start up.
init_doc = dict(_id="rs0", members=[dict(_id=0, host=f"{HOST}:27017")])
for (i, host) in enumerate(hosts):
    init_doc["members"].append(dict(_id=i + 1, host=f"{host}:27017"))  # type:ignore

print('\nWaiting for replica set')
name = "rs0"
con = MongoClient(f"{HOST}:27017", directConnection=True)
try:
    rs_status = con['admin'].command({'replSetGetStatus': 1})
    print(rs_status)
except OperationFailure:
    print("initializing replica set '%s' with configuration: %s"
          % (name, init_doc))
    for i in range(30):
        try:
            rs_status = con['admin'].command({'replSetInitiate': init_doc})
            print(rs_status)
            break
        except OperationFailure as e:
            print(e.message + " - will retry")
            time.sleep(1)

rs_status = con['admin'].command({'replSetGetStatus': 1})
print(rs_status)
print("Replica set '%s' initialized." % name)
con.close()

con = MongoClient(f"{HOST}:27017", replicaset=name)
con.admin.command('ping')
print('\n\nStarted Mongo Replica Set:')
print(con.topology_description)
print('\n\n')
con.close()

while 1:
    try:
        time.sleep(1)
    except KeyboardInterrupt:
        print('Closing Sessions')
        break

