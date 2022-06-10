# Raspberry Pi Notes

## Flash the Raspberry Pi SD Card

Install Raspberry Pi OS Lite 64-bit
https://www.raspberrypi.com/software/operating-systems/


## Enable SSH on the Raspberry Pi
https://phoenixnap.com/kb/enable-ssh-raspberry-pi

```bash
sudo touch /boot/ssh
```

## Install Requirements on MacOS

- Python 3.9
- Mongo Server 4.4


## Enable Internet Sharing on MacOS

System Preferences -> Sharing -> Internet Sharing

Share connection from Wifi to Ethernet

## Bootstrap the Raspberry Pi

```bash
scp bootstrap.sh <username>@192.168.2.2:bootstrap.sh
ssh <username>@192.168.2.2
```

```bash
bash bootstrap.sh
```

## Enable Host on MacOS

System Preferences -> Sharing -> Internet Sharing

Share connection from Ethernet to Ethernet

## Set up the services on all hosts

```bash
pip install -r requirements.txt
export MONGO_BINARY=<path-to-mongo-4.4-binary>
export RPI_USERNAME=<username>
export RPI_PASSWORD=<password>
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
