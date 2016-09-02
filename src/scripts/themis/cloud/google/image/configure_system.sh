#!/bin/bash

set -e

if [ $# -ne 4 ]
then
    echo "Usage: `basename $0` on_startup_script motd_banner_script version_number github_URL"
    exit $E_BADARGS
fi

ON_STARTUP=$1
MOTD_BANNER=$2
VERSION_NUMBER=$3
GITHUB_URL=$4

USER=`whoami`

if [ ! -f $ON_STARTUP ]
then
    echo "Script to run on start-up: $ON_STARTUP does not exist."
    exit 1
fi

if [ ! -f $MOTD_BANNER ]
then
    echo "MOTD banner script $MOTD_BANNER does not exist."
    exit 1
fi

# Set up some convenience symlinks
echo "Setting symlinks..."
if [ ! -L ~/script_runner ]; then
  ln -s ~/themis/src/scripts/themis/script_runner ~/script_runner
fi
if [ ! -L ~/job_runner ]; then
  ln -s ~/themis/src/scripts/themis/job_runner ~/job_runner
fi
if [ ! -L ~/valsort ]; then
  ln -s ~/themis/src/scripts/valsort ~/valsort
fi
if [ ! -L ~/networkbench ]; then
  ln -s ~/themis/src/tritonsort/benchmarks/networkbench ~/networkbench
fi
if [ ! -L ~/storagebench ]; then
  ln -s ~/themis/src/tritonsort/benchmarks/storagebench ~/storagebench
fi
if [ ! -L ~/mixediobench ]; then
  ln -s ~/themis/src/tritonsort/benchmarks/mixediobench ~/mixediobench
fi

# Change file limit to 4096
echo -e "$USER hard nofile 4096\n$USER soft nofile 4096" | sudo tee -a /etc/security/limits.conf

echo "Disabling requiretty for ssh..."
# Disable requiretty for ssh
sudo cat /etc/sudoers | sed -r 's/Defaults(\s)+requiretty/#&/' > ~/etcsudoers
sudo chown root:root ~/etcsudoers
sudo mv ~/etcsudoers /etc/sudoers
sudo chmod 440 /etc/sudoers

# Configure start-up script.
echo "Configuring startup script..."
STARTUP_SCRIPT=~/.on_startup.sh
cp $ON_STARTUP $STARTUP_SCRIPT
sudo chown $USER:$USER $STARTUP_SCRIPT
chmod u+x $STARTUP_SCRIPT

# Launch the startup script on boot
sudo sed -i "s/# By default this script does nothing./# By default this script does nothing.\nsu -s \/bin\/bash -l -c \/home\/$USER\/.on_startup.sh $USER 1> \/home\/$USER\/.startup.out 2> \/home\/$USER\/.startup.err/" /etc/rc.local

echo "Configuring MOTD banner..."
# Update version number in motd banner
sed -i "s/Themis Image VERSIONNUMBER/Themis Image $VERSION_NUMBER/" $MOTD_BANNER

# Copy MOTD banner
sudo mv ${MOTD_BANNER} /etc/motd
sudo chown root:root /etc/motd

# Load conf file settings into bash for convenience
THEMIS_BASH_CONFIG=~/.themis_bash_config
echo "source <(awk '/=/{print \"export \" \$1 \"=\" \$3}' cluster.conf)" >> ~/.themis_bash_config
echo "source <(awk '/=/{print \"export \" \$1 \"=\" \$3}' google.conf)" >> ~/.themis_bash_config
echo "source <(awk '/=/{print \"export \" \$1 \"=\" \$3}' node.conf)" >> ~/.themis_bash_config

echo "export PATH=\$PATH:/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin:/opt/aws/bin" >> ~/.themis_bash_config

# Store github URL
echo "export GITHUB_URL=${GITHUB_URL}" >> ~/.themis_bash_config

# Fix prompt
echo "export PS1='[\t] \u@\h:\w$ '" >> ~/.themis_bash_config

# Load this file from the bashrc file BEFORE it exits...
sudo sed -i "s/# for examples/# for examples\nsource ~\/.themis_bash_config/" ~/.bashrc

# Update gcloud components
sudo gcloud components update -q
