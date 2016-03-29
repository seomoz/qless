#! /bin/bash

set -e

sudo apt-get update

sudo apt-get install -y autoconf bison build-essential git g++ libssl-dev libyaml-dev \
    libreadline6-dev zlib1g-dev libncurses5-dev redis-server fontconfig libxml2-dev \
    libxslt-dev libffi-dev

# Rbenv
if [ ! -e /home/vagrant/.rbenv ]; then
    git clone https://github.com/sstephenson/rbenv.git /home/vagrant/.rbenv
    echo 'export PATH="/home/vagrant/.rbenv/bin:$PATH"' >> /home/vagrant/.bash_profile
    echo 'eval "$(rbenv init -)"' >> /home/vagrant/.bash_profile
    source /home/vagrant/.bash_profile
fi

if [ ! -e /home/vagrant/.rbenv/plugins/ruby-build ]; then
    # Rbenv build
    git clone https://github.com/sstephenson/ruby-build.git /home/vagrant/.rbenv/plugins/ruby-build
fi

# Rbenv permissions
chown -R vagrant:vagrant /home/vagrant/.rbenv

# PhantomJS for tests
version=phantomjs-1.7.0-linux-x86_64
wget http://phantomjs.googlecode.com/files/$version.tar.bz2
tar xjf $version.tar.bz2
sudo mv $version/bin/phantomjs /usr/local/bin/
echo 'export PATH=/vagrant/phantomjs/bin:$PATH' >> /home/vagrant/.bash_profile

# Dependencies
source /home/vagrant/.bash_profile
(
    cd /vagrant
    rbenv install
    gem install bundler --no-ri --no-rdoc
    rbenv rehash
    bundle install
    rbenv rehash

    # Build
    bundle exec rake core:build
)
