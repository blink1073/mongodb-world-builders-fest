set -ex

sudo apt update
sudo apt upgrade
wget -qO - https://www.mongodb.org/static/pgp/server-4.4.asc | sudo apt-key add -
echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/4.4 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.4.list
sudo apt-get update
sudo apt-get install -y mongodb-org


wget https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-aarch64.sh
bash Miniforge3-Linux-aarch64.sh -b

/home/silvester/miniforge3/bin/pip install psutil mtools pymongo python-dateutil ipython dask distributed msgpack==1.0.3 numpy==1.22.4 pandas==1.4.2
