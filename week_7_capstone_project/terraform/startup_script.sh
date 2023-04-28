#! /bin/bash

# Update the package list and upgrade all installed packages
sudo apt-get update
sudo apt-get upgrade -y
sudo apt-get install bzip2 libxml2-dev

# Install Anaconda
wget https://repo.anaconda.com/archive/Anaconda3-2023.03-1-Linux-x86_64.sh -O ~/anaconda.sh
bash ~/anaconda.sh -b -p $HOME/anaconda3
echo 'export PATH="$HOME/anaconda3/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
rm -f ~/anaconda.sh