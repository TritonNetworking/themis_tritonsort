# Running Themis within a Vagrant Virtual Machine

You can use [Vagrant][vagrant] to create a standalone virtual
machine that contains all the dependencies necessary to run Themis. Here's how:

First, you'll need to install Vagrant and [Berkshelf][berkshelf]. Vagrant will
be used to create the virtual machine and manage its provisioning with
[Chef][chef], and Berkshelf is used to manage Chef cookbook dependencies.

Once you've installed Vagrant and Berkshelf, run

`vagrant plugin install vagrant-berkshelf`

to install the Berkshelf plugin for Vagrant. Now, all you have to do is run

`vagrant up`

from within this directory and you'll have a virtual machine that you can log
into using `vagrant ssh`. Your source tree will be mounted in `/vagrant` within
the virtual machine.


[vagrant]: http://vagrantup.com/
[berkshelf]: http://berkshelf.com/
[chef]: http://www.opscode.com/chef/
