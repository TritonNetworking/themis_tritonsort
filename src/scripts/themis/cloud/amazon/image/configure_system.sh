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
ln -s ~/themis/src/scripts/themis/script_runner ~/script_runner
ln -s ~/themis/src/scripts/themis/job_runner ~/job_runner
ln -s ~/themis/src/scripts/valsort ~/valsort
ln -s ~/themis/src/tritonsort/benchmarks/networkbench ~/networkbench
ln -s ~/themis/src/tritonsort/benchmarks/storagebench ~/storagebench
ln -s ~/themis/src/tritonsort/benchmarks/mixediobench ~/mixediobench

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
sudo bash -c "echo 'su -s /bin/bash -l -c /home/$USER/.on_startup.sh $USER 1> /home/$USER/.startup.out 2> /home/$USER/.startup.err' >> /etc/rc.local"

echo "Configuring MOTD banner..."
# Update version number in motd banner
sed "s/Themis AMI VERSIONNUMBER/Themis AMI $VERSION_NUMBER/" <$MOTD_BANNER > ${MOTD_BANNER}_versioned

# Copy MOTD banner
sudo mv ${MOTD_BANNER}_versioned /etc/update-motd.d/30-banner
sudo chown root:root /etc/update-motd.d/30-banner
sudo chmod a+x /etc/update-motd.d/30-banner
sudo chmod g-w /etc/update-motd.d/30-banner

# Load conf file settings into bash for convenience
echo "source <(awk '/=/{print \"export \" \$1 \"=\" \$3}' cluster.conf)" >> ~/.bashrc
echo "source <(awk '/=/{print \"export \" \$1 \"=\" \$3}' amazon.conf)" >> ~/.bashrc
echo "source <(awk '/=/{print \"export \" \$1 \"=\" \$3}' node.conf)" >> ~/.bashrc

echo "export PATH=\$PATH:/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin:/opt/aws/bin" >> ~/.bashrc

# Store github URL
echo "export GITHUB_URL=${GITHUB_URL}" >> ~/.bashrc

# Fix prompt
echo "export PS1='[\t] \u@\h:\w$ '" >> ~/.bashrc
