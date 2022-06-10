# Raspberry Pi Notes

Started from 64bit Raspberry PI Lite

## Enable SSH on the Raspberry Pi
https://phoenixnap.com/kb/enable-ssh-raspberry-pi

sudo touch /boot/ssh

## Enable Sharing on MacOS

System Preferences -> Sharing -> Internet Sharing

Share connection from Wifi to Ethernet

## Bootstrap the Raspberry Pi

```bash
scp bootstrap.sh silvester@192.168.2.2:bootstrap.sh
ssh silvester@192.168.2.2
```

```bash
bash bootstrap.sh
```

## Set up the services on all hosts

```bash
pip install -r requirements.txt
python start_services.py
```

## Start the Demo Notebook

```bash
jupyter notebook nyc-taxi-dask.ipynb
```

## Stop the Cluster

```bash
python stop_hosts.py
````
