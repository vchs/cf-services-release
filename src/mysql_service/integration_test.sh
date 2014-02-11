#!/bin/bash
set -x
set -e

proj_root=$(dirname $(readlink -f $0))
WARDEN_PKG=/var/vcap/packages/warden/services_warden/warden
VCAP_MYSQL56=/var/vcap/packages/mysql56
MYSQL56_ALL_IN_ONE_ID="dd1e0eb9-fb29-430e-a1d1-55fedb6937e8"
MYSQL56_ALL_IN_ONE_FILE="mysql-5.6.13-packaging-all-in-one.tgz"
MYSQL56_CONFIG_ID="49647a59-1976-4137-b8f1-63ec92d68ae3"
MYSQL56_CONFIG_FILE="mysql-5.6.13-config-all-in-one.tgz"

install_blobs_file() {
  id=$1
  tar_file=$2
  dst_dir=$3

  mkdir -p $dst_dir
  cd $dst_dir
  prefix=`echo -n $id| sha1sum - | cut -c1-2`
  wget --user=agent --password=AgenT -nv \
    "http://blobs.sin2.beefstall.info/$prefix/$id" -O "$tar_file"
  tar zxvf $tar_file
  rm -rf $tar_file
}

# nats expects to write in /var/vcap/sys/run
mkdir -p /var/vcap/sys/run
#sudo chown -R vcap:vcap /var/vcap/sys/run

# prepare the configuration of wardenized mysql
mkdir -p /var/vcap/store
cp -r /var/vcap/jobs/* /var/vcap/store

install_blobs_file $MYSQL56_ALL_IN_ONE_ID $MYSQL56_ALL_IN_ONE_FILE $VCAP_MYSQL56
install_blobs_file $MYSQL56_CONFIG_ID $MYSQL56_CONFIG_FILE "/var/vcap/store"
mkdir -p /var/vcap/sys/log/warden

# dynamically generate the config for the warden server
cat > /tmp/warden_server.yml <<-EOF
---
server:
  container_klass: Warden::Container::Linux
  container_grace_time: 300
  unix_domain_permissions: 0777
  container_rootfs_path: /var/vcap/data/warden/rootfs
  container_depot_path: /var/vcap/data/warden/depot/
  container_limits_conf:
    nofile: 8192
    nproc: 512
    as: 4194304
  quota:
    disk_quota_enabled: false
  allow_nested_warden: true
logging:
  level: debug2

network:
  pool_start_address: 60.254.0.0
  pool_size: 256

user:
  pool_start_uid: 20000
  pool_size: 256
EOF

pushd $WARDEN_PKG
nohup bundle exec rake warden:start[/tmp/warden_server.yml] >> \
  /var/vcap/sys/log/warden/warden.stdout.log &
sleep 5
popd

cd $proj_root
rm -rf .bundle vendor/bundle
bundle install --deployment --path vendor/bundle
bundle exec rake ci:setup:rspec spec:integration
tar czvf reports.tgz spec/reports
