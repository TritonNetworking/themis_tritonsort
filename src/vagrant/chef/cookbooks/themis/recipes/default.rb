execute "apt-get update"

required_packages =
  ["g++", "gcc", "cmake", "cmake-curses-gui", "libboost-dev",
   "libboost-filesystem-dev", "libpq-dev", "libcppunit-dev",
   "libgsl0-dev", "libgoogle-perftools-dev", "google-perftools",
   "git", "zlib1g-dev", "libhiredis-dev", "libcurl4-openssl-dev",
   "make", "libjemalloc-dev", "libaio-dev", "redis-server",
   "python-pip", "python-matplotlib"]

required_packages.each do |package_name|
  package package_name do
    action :install
  end
end

bash "install-re2" do
  code <<-EOH
    wget https://re2.googlecode.com/files/re2-20131024.tgz
    tar xvzf re2-20131024.tgz
    cd re2
    make
    make test
    make install
    make testinstall
    rm -rf re2
    rm -f re2-20131024.tgz
    EOH
  not_if do
    File.exists?("/usr/local/lib/libre2.so")
  end
end

bash "install-gflags" do
  code <<-EOH
    wget https://gflags.googlecode.com/files/libgflags0_2.0-1_amd64.deb
    wget https://gflags.googlecode.com/files/libgflags-dev_2.0-1_amd64.deb
    dpkg -i libgflags0_2.0-1_amd64.deb
    dpkg -i libgflags-dev_2.0-1_amd64.deb
    rm -f libgflags0_2.0-1_amd64.deb
    rm -f libgflags-dev_2.0-1_amd64.deb
    EOH
  not_if do
    File.exists?("/usr/lib/libgflags.so")
  end
end

cookbook_file "/etc/ntp.conf" do
  source "ntp.conf"
  user "root"
  group "root"
  mode 0644
end

service "ntp" do
  action :restart
end

bash "install-python-dependencies" do
  code <<-EOH
    pip install -r /vagrant/scripts/requirements.txt
    EOH
end

# Make sure matplotlib is using the Agg backend to avoid dependencies on things
# like WX and GTK
directory "/home/vagrant/.matplotlib" do
  owner "vagrant"
  group "vagrant"
  mode 00775
end

cookbook_file "/home/vagrant/.matplotlib/matplotlibrc" do
  source "matplotlibrc"
  user "vagrant"
  group "vagrant"
  mode 00664
done
