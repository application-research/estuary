#! /bin/bash
set -eu
set -o pipefail

help()
{
  echo "Welcome to the estuary installation script!"
  echo
  echo "Command works as follows:"
  echo
  echo "  Script template ./prep-script.sh [option(s)]"
  echo "  Options:"
  echo "    -h this menu"
  echo "    -v verbose mode"
  echo "    -d data directory for estuary use"
  echo "    -p password you wish to use for estuary database"
  echo
  echo "Example: ./prep-script.sh -d /data -p estuary1234 -v"
}

install_package() {
  PKG=$1
  sudo apt -y install $PKG
}

POSITIONAL_ARGS=()
PASSWORD="estuary123"
DATADIR="/data"
VERBOSE=0

while [[ $# -gt 0 ]]; do
  case $1 in
    -p|--password)
      PASSWORD=$2
      shift # past argument
      shift # past value
      ;;
    -d|--datadir)
      DATADIR=$2
      shift # past argument
      shift # past value
      ;;
    -h)
      help
      exit 1
      ;;
    -v)
      VERBOSE=1
      shift # past argument
      ;;
    -*|--*)
      echo "Unknown option $1"
      exit 1
      ;;
    *)
      POSITIONAL_ARGS+=("$1") # save positional arg
      shift # past argument
      ;;
  esac
done

if [ $VERBOSE -eq 1 ]; then
  echo "Verbose is on ... "
fi

echo "Updating Ubuntu ..."
if [ $VERBOSE -eq 1 ]; then
  sudo apt update
else
  sudo apt update &> /dev/null
fi

install_package "hwloc"
install_package "jq"
install_package "make"
install_package "gcc"
install_package "pkg-config"
install_package "libtinfo5"

echo "Checking for go installation."
if [ ! -d "/usr/local/go" ]; then
  echo "Downloading and installing go ..."
  if [ $VERBOSE -eq 1 ]; then
    wget https://go.dev/dl/go1.17.linux-amd64.tar.gz
    sudo tar zxvf go1.17.linux-amd64.tar.gz -C /usr/local
  else
    wget https://go.dev/dl/go1.17.linux-amd64.tar.gz &> /dev/null
    sudo tar zxvf go1.17.linux-amd64.tar.gz -C /usr/local &> /dev/null
  fi
  sudo rm -f go1.17.linux-amd64.tar.gz &> /dev/null
else
  echo "Go is already installed."
fi

echo "GOHOME=/usr/local/go" >> ~/.bashrc
echo "PATH=$PATH:/usr/local/go/bin:/usr/local/estuary" >> ~/.bashrc
echo "FULLNODE_API_INFO=wss://api.chain.love" >> ~/.bashrc
echo "DATADIR=$DATADIR" >> ~/.bashrc
echo "PASSWORD=$PASSWORD" >> ~/.bashrc
export GOHOME=/usr/local/go
export PATH=$PATH:/usr/local/go:/usr/local/estuary
export FULLNODE_API_INFO=wss://api.chain.love
export DATADIR=$DATADIR
export PASSWORD=$PASSWORD
export ESTUARY_IP=`ip route get 8.8.8.8 | grep -oP "src \K[^ ]+"`
echo "ESTUARY_IP=$ESTUARY_IP" >> ~/.bashrc

echo "Installing libOpenCL and setting up necessary softlinks."
mkdir neo -p
cd neo

if [ $VERBOSE -eq 1 ]; then
  wget https://github.com/intel/compute-runtime/releases/download/19.07.12410/intel-gmmlib_18.4.1_amd64.deb
  wget https://github.com/intel/compute-runtime/releases/download/19.07.12410/intel-igc-core_18.50.1270_amd64.deb
  wget https://github.com/intel/compute-runtime/releases/download/19.07.12410/intel-igc-opencl_18.50.1270_amd64.deb
  wget https://github.com/intel/compute-runtime/releases/download/19.07.12410/intel-opencl_19.07.12410_amd64.deb
  wget https://github.com/intel/compute-runtime/releases/download/19.07.12410/intel-ocloc_19.07.12410_amd64.deb
  sudo dpkg -i *.deb
else
  wget https://github.com/intel/compute-runtime/releases/download/19.07.12410/intel-gmmlib_18.4.1_amd64.deb &> /dev/null
  wget https://github.com/intel/compute-runtime/releases/download/19.07.12410/intel-igc-core_18.50.1270_amd64.deb &> /dev/null
  wget https://github.com/intel/compute-runtime/releases/download/19.07.12410/intel-igc-opencl_18.50.1270_amd64.deb &> /dev/null
  wget https://github.com/intel/compute-runtime/releases/download/19.07.12410/intel-opencl_19.07.12410_amd64.deb &> /dev/null
  wget https://github.com/intel/compute-runtime/releases/download/19.07.12410/intel-ocloc_19.07.12410_amd64.deb &> /dev/null
  sudo dpkg -i *.deb &> /dev/null
fi

ubuntuversion=`cat /etc/issue`
IFS=' '
read -a strattr <<< "$ubuntuversion"
IFS='.'
read -a majorv <<< ${strattr[1]}
if [ ${majorv[0]} = "20" ]; then
  sudo ln -fs /usr/lib/x86_64-linux-gnu/libhwloc.so.15.1.0 /usr/lib/libhwloc.so
  sudo ln -fs /usr/lib/x86_64-linux-gnu/libOpenCL.so.1.0.0 /usr/lib/libOpenCL.so
  echo "Major Ubuntu version is 20"
elif [ ${majorv[0]} = "22" ]; then
  sudo ln -fs /usr/lib/x86_64-linux-gnu/libhwloc.so.15.5.2 /usr/lib/libhwloc.so
  sudo ln -fs /usr/lib/x86_64-linux-gnu/libOpenCL.so.1.0.0 /usr/lib/libOpenCL.so
  echo "Major Ubuntu version is 22"
else
  echo "OS not supported at this time."
fi

echo "Checking for postgresql ... "
sudo apt -y install postgresql postgresql-contrib
  sudo systemctl start postgresql.service
  sudo systemctl enable postgresql.service

echo "Downloading estuary ... "
if [ ! -d /usr/local/estuary ]; then
  if [ $VERBOSE -eq 1 ]; then
    sudo git clone https://github.com/application-research/estuary /usr/local/estuary
  else
    sudo git clone https://github.com/application-research/estuary /usr/local/estuary &> /dev/null
  fi
fi



sudo -u postgres psql estuary < /dev/null || (
  echo "Configuring estuary database ..."
  sudo -u postgres createuser -d -s estuary
  sudo -u postgres psql -c "ALTER USER estuary PASSWORD '$PASSWORD';"
  sudo -u postgres createdb estuary
  echo "Setting estuary database password to: $PASSWORD"
  echo "Establishing $DATADIR as the estuary data store"
)

source ~/.bashrc
cd /usr/local/estuary

echo "Building estuary, now is a good time to go get coffee :)"
if [ $VERBOSE -eq 1 ]; then
  sudo --preserve-env=PATH make clean all
else
  sudo --preserve-env=PATH make clean all &> /dev/null
fi

if [ ! -d $DATADIR ]; then
  sudo mkdir -p $DATADIR
fi

if [ ! -f /usr/local/estuary/token.file ]; then
	sudo --preserve-env=PASSWORD,DATADIR bash -c '/usr/local/estuary/estuary --datadir='$DATADIR' --database="postgres=host=127.0.0.1 user=estuary password='$PASSWORD' dbname=estuary" setup --username user2 --password password > /usr/local/estuary/token.file'
fi

sudo bash -c 'echo "#! /bin/bash" > /usr/local/estuary/estuary_start.bash'
sudo bash -c 'echo "ulimit -n 10000" >> /usr/local/estuary/estuary_start.bash'
sudo bash -c 'echo "sysctl -w net.core.rmem_max=2500000" >> /usr/local/estuary/estuary_start.bash'
sudo --preserve-env=DATADIR,PASSWORD bash -c 'echo "/usr/local/estuary/estuary --datadir='$DATADIR' --database=\"postgres=host=127.0.0.1 user=estuary password='$PASSWORD' dbname=estuary\"" >> /usr/local/estuary/estuary_start.bash'
sudo chmod 755 /usr/local/estuary/estuary_start.bash
sudo --preserve-env=PATH make install-estuary-service
sudo sed -i.bak 's/ExecStart.*$/ExecStart=\/usr\/local\/estuary\/estuary_start.bash/g' /etc/systemd/system/estuary.service
sudo systemctl start estuary.service
sudo systemctl enable estuary.service

echo "Setting up estuary UI."
sudo git clone https://github.com/application-research/estuary-www /usr/local/estuary-www
cd /usr/local/estuary-www

echo "Checking for npm ... "
sudo apt -y install npm

echo "Updating npm ... "

if [ $VERBOSE -eq 1 ]; then
  sudo npm install -g n
  sudo n stable
  sudo npm upgrade
  sudo npm install -g webpack-dev-server
else
  sudo npm install -g n &> /dev/null
  sudo n stable &> /dev/null
  sudo npm upgrade &> /dev/null
  sudo npm install -g webpack-dev-server &> /dev/null
fi

sudo bash -c 'echo "#! /bin/bash" > /usr/local/estuary-www/start_www.bash'
sudo bash -c 'echo "cd /usr/local/estuary-www/" >> /usr/local/estuary-www/start_www.bash'
sudo bash -c 'echo "npm run dev" >> /usr/local/estuary-www/start_www.bash'
sudo chmod 755 /usr/local/estuary-www/start_www.bash
sudo bash -c 'echo "[Unit]" > /etc/systemd/system/estuary-www.service'
sudo bash -c 'echo "" >> /etc/systemd/system/estuary-www.service'
sudo bash -c 'echo "[Service]" >> /etc/systemd/system/estuary-www.service'
sudo bash -c 'echo "ExecStart=/usr/local/estuary-www/start_www.bash" >> /etc/systemd/system/estuary-www.service'
sudo bash -c 'echo "Restart=always" >> /etc/systemd/system/estuary-www.service'
sudo bash -c 'echo "RestartSec=10" >> /etc/systemd/system/estuary-www.service'
sudo bash -c 'echo "" >> /etc/systemd/system/estuary-www.service'
sudo bash -c 'echo "[Install]" >> /etc/systemd/system/estuary-www.service'
sudo bash -c 'echo "WantedBy=multi-user.target" >> /etc/systemd/system/estuary-www.service'

sudo --preserve-env=ESTUARY_IP sed -i.bak 's/localhost/'"$ESTUARY_IP"'/g' /usr/local/estuary-www/package.json

echo "Reloading daemons, enabling and starting estuary web UI."

sudo systemctl daemon-reload
sudo systemctl enable estuary-www.service
sudo systemctl start estuary-www.service
