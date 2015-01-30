#! /bin/bash

set -e

# JDK 7
sudo add-apt-repository -y ppa:webupd8team/java
echo debconf shared/accepted-oracle-license-v1-1 select true | sudo debconf-set-selections
sudo apt-get update
sudo apt-get install -y oracle-java7-installer

# Some dependencies
sudo apt-get update
sudo apt-get install -y autoconf bison build-essential git g++ libssl-dev libyaml-dev \
    libreadline6-dev zlib1g-dev libncurses5-dev redis-server fontconfig libxml2-dev \
    libxslt-dev maven

# Build qless-core
(
    cd /vagrant
    git submodule update --init --recursive
    make -C qless-java/src/qless-core
)
