# -*- mode: ruby -*-
# vi: set ft=ruby :

$setup = <<SCRIPT
set -e

# Update apt
apt-get update

# Install the things we need to build
apt-get install -y autoconf automake
apt-get install -y build-essential scons
apt-get install -y git-core
apt-get install -y libtool
apt-get install -y telnet
apt-get install -y screen
apt-get install -y htop strace
apt-get install -y libncurses5 libncurses5-dev
apt-get install -y openssl libssl-dev

# Compile Erlang from source
if [ ! -f /usr/local/bin/erl ]; then
  pushd /tmp

  if [ ! -f otp_src_R15B03-1.tar.gz]; then
      # Download Erlang
      wget --progress=dot -e dotbytes=1M http://www.erlang.org/download/otp_src_R15B03-1.tar.gz

      # Untar it
      tar xvzf otp_src_R15B03-1.tar.gz
  fi

  # Compile it
  pushd otp_src_R15B03
  ./configure
  make
  make install
  popd

  popd
fi

# Compile bloomd from source
if [ ! -f /usr/bin/bloomd ]; then
    pushd /tmp

    # Get bloomd
    if [ ! -d bloomd ]; then
        git clone --depth 1 https://github.com/armon/bloomd
    fi

    # Build it
    cd bloomd
    scons

    # Move it into place
    chmod +x bloomd
    cp bloomd /usr/bin

    # Cleanup
    cd ../
    rm -R bloomd

    popd
fi

# Set file limits
RES=`grep 32000 /home/ubuntu/.bashrc`
if [ ! $RES ]; then
    echo "*	soft    nofile 	32000" >> /etc/security/limits.conf
    echo "*	hard nofile 	32000" >> /etc/security/limits.conf
    echo "ulimit -n 32000" >> /home/ubuntu/.bashrc
fi


SCRIPT

Vagrant.configure("2") do |config|
  config.vm.box = "precise64"
  config.vm.provision :shell, :inline => $setup
  config.vm.network :forwarded_port,
    guest: 22,
    host:  2456,
    id: "ssh"

  config.vm.provider :vmware_fusion do |p|
    p.vmx["numvcpus"] = "2"
    p.vmx["coresPerSocket"] = "2"
    p.vmx["memsize"] = "2048"
  end

  config.vm.provider :aws do |aws|
    aws.access_key_id = ""
    aws.secret_access_key = ""
    aws.keypair_name = "kiipkey"
    aws.ssh_private_key_path = "/Users/armon/projects/kiip/kiipweb/.chef/kiip-ssh.pem"
    aws.security_groups = ["cluster"]
    aws.instance_type = "m1.large"
    aws.availability_zone = "us-east-1a"

    aws.ami = "ami-b6089bdf"
    aws.ssh_username = "ubuntu"
  end

  config.vm.define :n1 do |x|
      x.vm.network :private_network, ip: "33.33.36.10"

      x.vm.provider :aws do |aws|
          aws.tags["Name"] = "bloomd_ring_n1"
      end
  end

  config.vm.define :n2 do |y|
      y.vm.network :private_network, ip: "33.33.36.11"

      y.vm.provider :aws do |aws|
          aws.tags["Name"] = "bloomd_ring_n2"
      end
  end

  config.vm.define :n3 do |z|
      z.vm.network :private_network, ip: "33.33.36.12"

      z.vm.provider :aws do |aws|
          aws.tags["Name"] = "bloomd_ring_n3"
      end
  end
end
