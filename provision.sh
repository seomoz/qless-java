#! /bin/bash

set -e

# JDK 7
sudo add-apt-repository -y ppa:webupd8team/java
echo debconf shared/accepted-oracle-license-v1-1 select true | sudo debconf-set-selections
sudo apt-get update
sudo apt-get install -y oracle-java7-installer

# Some dependencies
sudo apt-get update
sudo apt-get install -y autoconf bison build-essential git g++ gnupg-agent \
    libssl-dev libyaml-dev libreadline6-dev zlib1g-dev libncurses5-dev \
    redis-server fontconfig libxml2-dev libxslt-dev maven

# Build qless-core
(
    cd /vagrant
    git submodule update --init --recursive
    make -C src/qless-core
)

# For deploying
mkdir -p /vagrant/.gnupg
ln -s /vagrant/.gnupg/ /home/vagrant/.gnupg
echo >> ~/.bash_profile <<PROFILE
eval $(gpg-agent --daemon --no-grab --write-env-file $HOME/.gpg-agent-info)
export GPG_TTY=$(tty)
export GPG_AGENT_INFO'
cd /vagrant
PROFILE
mkdir -p /home/vagrant/.m2
ln -s /vagrant/settings-security.xml /home/vagrant/.m2/settings-security.xml
ln -s /vagrant/settings.xml /home/vagrant/.m2/settings.xml
mkdir -p /vagrant/.m2/repository
ln -s /vagrant/.m2/repository /home/vagrant/.m2/repository
