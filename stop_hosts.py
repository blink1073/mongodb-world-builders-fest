import os

from paramiko import SSHClient

PASSWORD = os.environ['RPI_PASSWORD']


def stop_host(host):
    print('\n\nStopping host:', host)
    client = SSHClient()
    client.load_system_host_keys()
    client.connect(host, username='silvester', password=PASSWORD)
    client.exec_command('sudo shutdown now', get_pty=True)
    client.close()
    print('Stopped host:', host, '\n\n')



with open('host_list.txt') as fid:
    hosts = [host.strip() for host in fid.readlines()]

for host in hosts:
    stop_host(host)
