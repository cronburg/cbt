import configparser
from socket import gethostname
import logging
import settings
import os
import monitoring
import time

from cluster.ceph import Ceph
from benchmark import Benchmark
import common

logger = logging.getLogger("cbt")

class Fio(Benchmark):

    fio_defaults = {
        'cmd_path'      : '/usr/bin/fio',
        'runtime'       : None,
        'ramp_time'     : None,
        'iodepth'       : 16,
        'numjobs'       : 1,
        'end_fsync'     : 0,
        'mode'          : 'write',
        'rwmixread'     : 50,
        'log_avg_msec'  : None,
        'ioengine'      : 'rbd',
	'clientname'    : 'admin',
        'op_size'       : 4194304,
        'rate_iops'     : None }
        
    cbt_defaults = {
        'idle_sleep'    : 60,
        'pgs'           : 2048,
        'vol_size'      : 65536,
        'vol_order'     : 22,
        'pool_profile'  : 'default',
        'volumes_per_client'    : 1,
        'procs_per_volume'      : 1,
        'random_distribution'   : None,
        'use_existing_volumes'  : False }

    def __init__(self, cluster, config):
        super(Fio, self).__init__(cluster, config)

        # Syntactic ease of access to config parameters:
	self.__dict__.update(Fio.fio_defaults)
	self.__dict__.update(Fio.cbt_defaults)
	self.__dict__.update(config)
        
        # Build the global section of the fio job file:
	fiocfg = self.fiocfg = configparser.ConfigParser()
        fiocfg['global'] = config.get('global')
        for (k,v) in Fio.fio_defaults.items():
            if v is None: continue
            if not (k in fiocfg['global'].keys() or k in config.keys()):
                fiocfg['global'][k] = str(v)

        self.total_procs = self.procs_per_volume * self.volumes_per_client * len(settings.getnodes('clients').split(','))

        self.run_dir = self.build_path(self.run_dir)
        self.out_dir = self.build_path(self.archive_dir)

        # Make the file names string (repeated across volumes)
        self.names = ''
        for i in xrange(self.procs_per_volume):
            self.names += '--name=librbdfio-`hostname -s`-%d ' % i

    def build_path(self, root):
    	""" Construct the directory structure for fio output and archived files """
        return os.path.join(
            root,
            'osd_ra-%08d'           % int(self.osd_ra),
            'op_size-%08d'          % int(self.op_size),
            'concurrent_procs-%03d' % int(self.total_procs),
            'iodepth-%03d'          % int(self.iodepth),
            self.mode)

    def exists(self):
        if os.path.exists(self.out_dir):
            logger.info('Skipping existing test in %s.', self.out_dir)
            return True
        return False

    def initialize(self):
        super(Fio, self).initialize()

        logger.info('Running scrub monitoring.')
        monitoring.start("%s/scrub_monitoring" % self.run_dir)
        self.cluster.check_scrub()
        monitoring.stop()

        logger.info('Pausing for %d s for idle monitoring.' % self.idle_sleep)
        monitoring.start("%s/idle_monitoring" % self.run_dir)
        time.sleep(self.idle_sleep)
        monitoring.stop()

        common.sync_files('%s/*' % self.run_dir, self.out_dir)

        self.mkimages()

        # Create the run directory
        common.make_remote_dir(self.run_dir)
	
	"""
        # populate the fio files
        ps = []
        logger.info('Attempting to populating fio files...')
        if (self.use_existing_volumes == False):
          for i in xrange(self.volumes_per_client):
              pre_cmd = 'sudo %s --ioengine=rbd --clientname=admin --pool=%s --rbdname=cbt-librbdfio-`hostname -s`-%d --invalidate=0  --rw=write --numjobs=%s --bs=4M --size %dM %s' % (self.cmd_path, self.poolname, i, self.numjobs, self.vol_size, self.names)
              p = common.pdsh(settings.getnodes('clients'), pre_cmd)
              ps.append(p)
          for p in ps:
              p.wait()
        """
        return True

    def run(self):
        super(Fio, self).run()

        # We'll always drop caches for rados bench
        self.dropcaches()

        # dump the cluster config
        self.cluster.dump_config(self.run_dir)

        monitoring.start(self.run_dir)

        time.sleep(5)

        # Run the backfill testing thread if requested
        if 'recovery_test' in self.cluster.config:
            recovery_callback = self.recovery_callback
            self.cluster.create_recovery_test(self.run_dir, recovery_callback)

        logger.info('Running rbd fio %s test.', self.mode)
        ps = []
        for i in xrange(self.volumes_per_client):
            fio_cmd = self.mkfiocmd(i)
            p = common.pdsh(settings.getnodes('clients'), fio_cmd)
            ps.append(p)
        for p in ps:
            p.wait()
        # If we were doing recovery, wait until it's done.
        if 'recovery_test' in self.cluster.config:
            self.cluster.wait_recovery_done()

        monitoring.stop(self.run_dir)

        # Finally, get the historic ops
        self.cluster.dump_historic_ops(self.run_dir)
        common.sync_files('%s/*' % self.run_dir, self.out_dir)

    def mkfiocmd(self, volnum):
        rbdname = 'cbt-librbdfio-`hostname -s`-%d' % volnum
        out_file = '%s/output.%d' % (self.run_dir, volnum)

        local_job_file  = os.path.join(self.archive_dir, 'fio_job_file.%d' % volnum)
        remote_job_file = os.path.join(self.run_dir,     'fio_job_file.%d' % volnum)

        glb = self.fiocfg['global']
        glb['rw'] = str(self.mode)
        if (self.mode == 'readwrite' or self.mode == 'randrw'):
            glb['rwmixread'] = str(self.rwmixread)
            glb['rwmixwrite'] = str(self.rwmixwrite)
        if self.runtime is not None:
            glb['runtime'] = str(self.runtime)
        if self.ramp_time is not None:
            glb['ramp_time'] = str(self.ramp_time)
        glb['numjobs'] = str(self.numjobs)
        glb['direct'] = str(1)
        glb['bs'] = '%dB' % self.op_size
        glb['iodepth'] = str(self.iodepth)
        glb['end_fsync'] = str(self.end_fsync)
        
        job = self.fiocfg.add_section('job')
        job['write_iops_log']   = out_file
        job['write_bw_log']     = out_file
        job['write_lat_log']    = out_file
        
	if 'recovery_test' in self.cluster.config:
            glb.set('time_based')
        if self.random_distribution is not None:
            glb['random_distribution'] = self.random_distribution
        if self.log_avg_msec is not None:
            glb['log_avg_msec'] = str(self.log_avg_msec)
        if self.rate_iops is not None:
            glb['rate_iops'] = str(self.rate_iops)

        with open(local_job_file, 'w') as fp:
            self.fiocfg.write(fp, False)
        common.pdcp(settings.getnodes('clients'), '', local_job_file, remote_job_file).communicate()

        return 'sudo %s %s' % (self.cmd_path, remote_job_file)

    def mkimages(self):
        monitoring.start("%s/pool_monitoring" % self.run_dir)
        if (self.use_existing_volumes == False):
          self.cluster.rmpool(self.poolname, self.pool_profile)
          self.cluster.mkpool(self.poolname, self.pool_profile)
          for node in settings.getnodes('clients').split(','):
              for volnum in xrange(0, self.volumes_per_client):
                  node = node.rpartition("@")[2]
                  self.cluster.mkimage('cbt-librbdfio-%s-%d' % (node,volnum), self.vol_size, self.poolname, self.vol_order)
        monitoring.stop()

    def recovery_callback(self):
        common.pdsh(settings.getnodes('clients'), 'sudo killall -9 fio').communicate()

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(LibrbdFio, self).__str__())

