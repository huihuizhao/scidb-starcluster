import os
import time
import random
import string
from starcluster.clustersetup import DefaultClusterSetup
from starcluster.exception import RemoteCommandFailed
from starcluster.logger import log


SCIDB_VERSION = 15.7
SCIDB_REVISION = 8628
POSTGRES_VERSION = '9.1'
SCIDB_INSTALL_PATH = '/opt/scidb/$SCIDB_VER'

DEFAULT_USERNAME = 'scidb'
DEFAULT_REPOSITORY = 'https://github.com/suhailrehman/scidb.git'
DEFAULT_BRANCH = None
DEFAULT_SHIM_PACKAGE_URI = 'https://s3.amazonaws.com/scidb-packages/shim_15.7_amd64.deb'
DEFAULT_DIRECTORY = '/home/scidb/scidbtrunk'
DEFAULT_CLIENTS = '0.0.0.0/0'
DEFAULT_BUILD_TYPE = 'RelWithDebInfo'
DEFAULT_BUILD_THREADS = 4

DEFAULT_REDUNDANCY = 0
DEFAULT_INSTANCES_PER_NODE = 1

# http://www.scidb.org/forum/viewtopic.php?f=11&t=530
# Documentation specified Postgres 8.4; I moved to 9.1
REQUIRED_PACKAGES = ['build-essential', 'cmake', 'libboost1.48-all-dev',
                     'postgresql-9.1', 'libpqxx-3.1', 'libpqxx3-dev',
                     'libprotobuf7', 'libprotobuf-dev', 'protobuf-compiler',
                     'doxygen', 'flex', 'bison', 'libxerces-c-dev',
                     'libxerces-c3.1', 'liblog4cxx10', 'liblog4cxx10-dev',
                     'libcppunit-1.12-1', 'libcppunit-dev', 'libbz2-dev',
                     'postgresql-contrib-9.1', 'libconfig++8',
                     'libconfig++8-dev', 'libconfig8-dev', 'subversion',
                     'libreadline6-dev', 'libreadline6', 'python-paramiko',
                     'python-crypto', 'xsltproc', 'liblog4cxx10-dev',
                     'subversion', 'expect', 'openssh-server', 'openssh-client',
                     'git-svn', 'gdebi'] # Shim uses gdebi

class SciDBInstaller(DefaultClusterSetup):


    master_user_pubkey = ""

    def __init__(self,
                 username=DEFAULT_USERNAME,
                 password=''.join(random.sample(string.lowercase+string.digits, 20)),
                 repository=DEFAULT_REPOSITORY,
                 branch=DEFAULT_BRANCH,
                 shim_uri=DEFAULT_SHIM_PACKAGE_URI,
                 directory=DEFAULT_DIRECTORY,
                 clients=DEFAULT_CLIENTS,
                 build_type=DEFAULT_BUILD_TYPE,
                 build_threads=DEFAULT_BUILD_THREADS,
                 redundancy=DEFAULT_REDUNDANCY,
                 instances_per_node=DEFAULT_INSTANCES_PER_NODE):
        super(SciDBInstaller, self).__init__()

        self.username = username
        self.password = password
        self.repository = repository
        self.branch = branch
        self.shim_uri = shim_uri
        self.directory = directory
        self.clients = clients
        self.build_type = build_type
        self.build_threads = build_threads
        self.redundancy = redundancy
        self.instances_per_node = instances_per_node

    def _set_up_node(self, master, node):
        log.info("1   Begin configuration {}".format(node.alias))

        log.info('*   Adding scidb user pubkey to root passwordless SSH')
        node.ssh.execute('echo {} >> /root/.ssh/authorized_keys'.format(self.master_user_pubkey[0]))

    def _set_ownership(self, master, node):
        log.info('*   Setting home directory owner to scidb on node {}'.format(node.alias))
        try:
          node.ssh.execute('chown -R scidb /home/scidb')
          node.ssh.execute('chmod 700 /home/scidb/.ssh')
        except RemoteCommandFailed as e:
          if node != master:
            pass
          else:
            raise e

    def run(self, nodes, master, user, user_shell, volumes):
        super(SciDBInstaller, self).run(nodes, master, user, user_shell, volumes)

        aliases = ' '.join(map(lambda node: node.alias, nodes))

        log.info('Beginning SciDB cluster configuration')
        log.info('*   Allowing root passwordless ssh for the scidb user "{}"'.format(self.username))

        self.master_user_pubkey = master.ssh.execute('cat /home/{}/.ssh/id_rsa.pub'.format(self.username))

        log.info('*   Public Key: '.format(self.master_user_pubkey))

        #Install SciDB root key
        [self.pool.simple_job(self._set_up_node, (master, node), jobid=node.alias) for node in nodes]
        self.pool.wait(len(nodes))

        master.ssh.switch_user(user)        

        log.info('    * Prepare')
        self._execute(master, 'deployment/deploy.sh scidb_prepare scidb "{password}" mydb mydb mydb {directory}/db {instances} default {redundancy} {aliases}'.format(
                instances=self.instances_per_node,
                redundancy=self.redundancy,
                password=self.password,
                directory=self.directory,
                aliases=aliases))


        log.info('7   Start SciDB')
        #log.info('    * Initialize Catalogs')
        #self._execute(master, '/opt/scidb/14.8/bin/scidb.py initall mydb -f')
        log.info('    * Starting SciDB on all instances')
        self._execute(master, '/opt/scidb/15.7/bin/scidb.py startall mydb')

        log.info('    * Starting Shim on Master')
        self._execute(master, '/opt/scidb/15.7/bin/shim')

        log.info('End SciDB cluster configuration')



    def _add_user(self, master, nodes):
        uid, gid = self._get_new_user_id(self.username)
        map(lambda node: self.pool.simple_job(self.__add_user_to_node, (uid, gid, node), jobid=node.alias), nodes)
        self.pool.wait(numtasks=len(nodes))
        #self._setup_cluster_user(self.username)
        master.generate_key_for_user(self.username, auth_new_key=True, auth_conn_key=True)
        master.add_to_known_hosts(self.username, nodes)

    def __add_user_to_node(self, uid, gid, node):
        node.add_user(self.username, uid, gid, '/bin/bash')
        node.ssh.execute('echo -e "{username}:{password}" | chpasswd'.format(
            username=self.username, password=self.password))

    def _add_directory(self, node, path):
        node.ssh.execute('mkdir {}'.format(path))
        node.ssh.execute('chown {username} {path}'.format(username=self.username, path=path))
        node.ssh.execute('chgrp {username} {path}'.format(username=self.username, path=path))

    def _add_swapfile(self, node):
        node.ssh.execute('sudo dd if=/dev/zero of=/mnt/swapfile bs=1024 count=2048k')
        node.ssh.execute('sudo mkswap /mnt/swapfile')
        node.ssh.execute('sudo swapon /mnt/swapfile')

    def _add_environment(self, node, path):
        with node.ssh.remote_file(path, 'a') as descriptor:
            descriptor.write('export SCIDB_VER={}\n'.format(SCIDB_VERSION))
            descriptor.write('export SCIDB_INSTALL_PATH={}\n'.format(SCIDB_INSTALL_PATH))
            descriptor.write('export SCIDB_BUILD_PATH=$SCIDB_SOURCE_PATH/stage/build\n')
            descriptor.write('export SCIDB_SOURCE_PATH={}'.format(self.directory))
            descriptor.write('export PATH=$SCIDB_INSTALL_PATH/bin:$PATH\n')
            #descriptor.write('export PATH={}/stage/install/bin:$PATH\n'.format(self.directory))
            #descriptor.write('export LD_LIBRARY_PATH={}/stage/install/lib:$LD_LIBRARY_PATH\n'.format(self.directory))
            descriptor.write('export SCIDB_BUILD_TYPE={}\n'.format(self.build_type))

    def _ensure_revision(self, node):
        self._execute(node, 'mv .git temp.git') #TODO
        with node.ssh.remote_file(os.path.join(self.directory, 'revision'), 'w') as descriptor:
            descriptor.write(str(SCIDB_REVISION))

    def _distribute_libraries(self, master, node):
        # Awesome!  SciDB has hardcoded paths (/opt/scidb/*)
        node.ssh.execute('mkdir -p /opt/scidb/{}'.format(SCIDB_VERSION))
        master.ssh.execute('scp -r {directory}/stage/build/debian/scidb-{version}-plugins{directory}/stage/install/* {alias}:/opt/scidb/{version}'.format(
            directory=self.directory,
            alias=node.alias,
            version=SCIDB_VERSION))

    def _copy_deployment(self, master, node):
        master.ssh.execute('scp -r {}/stage/install/* {}:/opt/scidb/{}'.format(self.directory, node.alias, SCIDB_VERSION))

    def _set_root_ssh_config(self, node, path='/root/.ssh/config'):
        with node.ssh.remote_file(path, 'w') as descriptor:
            descriptor.write('Host *\n'
                             '   StrictHostKeyChecking no\n'
                             '   UserKnownHostsFile=/dev/null\n'
                             '   IdentityFile ~/.ssh/id_rsa\n'
                             '   User root\n'
                             '\n'
                             'Host *\n'
                             '   StrictHostKeyChecking no\n'
                             '   UserKnownHostsFile=/dev/null\n'
                             '   IdentityFile /home/scidb/.ssh/id_rsa\n'
                             '   User scidb\n')
        node.ssh.execute('chmod 600 {}'.format(path))

    def _set_postgres_listener(self, node, listeners, path='/etc/postgresql/{version}/main/postgresql.conf', version=POSTGRES_VERSION):
        node.ssh.execute(r'sed -i "s/^\s*\#\?\s*listen_addresses\s*=\s*''.*\?''/listen_addresses = \'{listeners}\'/ig" {path}'.format(
            listeners=listeners,
            path=path.format(version=version)))
        node.ssh.execute('sudo service postgresql restart')

    def _add_host_authentication(self, node, authentication, path='/etc/postgresql/{version}/main/pg_hba.conf', version=POSTGRES_VERSION):
        with node.ssh.remote_file(path.format(version=version), 'a') as descriptor:
            descriptor.write(authentication + '\n')
        node.ssh.execute('sudo service postgresql restart')

    def _execute(self, node, command):
        node.ssh.execute('cd {} && {}'.format(self.directory, command))