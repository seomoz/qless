# Encoding: utf-8
# -*- mode: ruby -*-
# vi: set ft=ruby :

ENV['VAGRANT_DEFAULT_PROVIDER'] = 'virtualbox'

# http://docs.vagrantup.com/v2/
Vagrant.configure('2') do |config|
  config.vm.box = 'ubuntu/trusty64'
  config.vm.hostname = 'qless'
  config.ssh.forward_agent = true

  # Part of provisioning is cloning a couple of private repos, so this is to
  # enable key forwarding during provisioning
  config.vm.provision :shell, inline:
    'echo \'Defaults env_keep += "SSH_AUTH_SOCK"\' > /etc/sudoers.d/ssh-auth-sock; ' +
    'chmod 0440 /etc/sudoers.d/ssh-auth-sock'

  config.vm.provider :virtualbox do |vb|
    vb.customize ["modifyvm", :id, "--memory", "1024"]
    vb.customize ["modifyvm", :id, "--cpus", "2"]
  end

  config.vm.provision :shell, path: 'provision.sh', privileged: false
end
