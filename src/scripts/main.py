#!/usr/bin/python

import sys
from config import *
from utils import *
import time
from sets import Set
import pprint as pp

def prepare_folders(conf):
    print '-----------------'
    print 'Preparing folders'
    print '-----------------'
    
    make_dirs(conf['run_dir'], "all", conf)
    if conf['onEC2']:
        make_dirs(conf['test_dir'],'all',conf)
    else:
        make_dirs(conf['test_dir'])

    results_dir = conf['run_dir'] +'/results'
    make_dirs(results_dir, "all", conf)
    conf['results_dir'] = results_dir

    hists_dir = conf['run_dir'] +'/histograms'
    make_dirs(results_dir, "all", conf)
    conf['hists_dir'] = hists_dir
    
    clients_dir = conf['run_dir']+'/clients'
    make_dirs(clients_dir, "all", conf)
    conf['clients_dir'] = clients_dir

    clients_dir = conf['run_dir']+'/servers'
    make_dirs(clients_dir, "all", conf)
    conf['servers_dir'] = clients_dir
    
    out_dir = conf['run_dir'] + "/out"
    make_dirs(out_dir, "all", conf)
    conf['out_dir'] = out_dir
    
def copy_files(conf):
    print '-------------'
    print 'Copying files'
    print '-------------'
    
    cmd = "cp -r %s/* %s"%(conf['run_dir'], conf['test_dir'])
    runCmdParallel(cmd, "all", conf, True)
    
    #cmd = "cp -r %s %s"%(conf['results_dir'], conf['test_dir'])
    #runCmdParallel(cmd, "servers", conf, True)
    
    # commands = []
    # for i,host in enumerate(conf['clientMachines']):
    #     cmd1 = "cp -r %s %s"%(conf['results_dir'], conf['test_dir'])
    #     #run_cluster_cmd(cmd, host, conf, True)
    #
    #     cmd2 = "cp -r %s %s"%(conf['hists_dir'], conf['test_dir'])
    #     #run_cluster_cmd(cmd, host, conf, True)
    #
    #     cmd3 = "cp -r %s %s"%(conf['clients_dir'], conf['test_dir'])
    #     #run_cluster_cmd(cmd, host, conf, True)
    #
    #     commands.append({'host':host,'cmds':[cmd1, cmd2, cmd3]})
    # runCommandsParallelForHost(commands, conf, True)
        
    # if conf['onEC2']:
    #     #cmd = "cd %s/..; tar -zcf results.tgz results/%s/run_%s"%(conf['exp_dir'],conf['test_id'],conf['test_run'])
    #     #print "cmd: ", cmd
    #     #runCmdParallel(cmd, "all", conf, False)
    #
    #     commands = []
    #     for i,host in enumerate(get_hosts_list(conf,"all")):
    #         tgzFile = 'results-%d.tgz'%(i+1)
    #         cmd1 = "cd %s/..; tar -zcf %s results/%s/run_%s"%(conf['exp_dir'],tgzFile,conf['test_id'],conf['test_run'])
    #
    #         commands.append({'host':host,'cmds':[cmd1]})
    #     runCommandsParallelForHost(commands, conf, True)
    #
    #     commands = []
    #     for i,host in enumerate(get_hosts_list(conf,"all")):
    #         tgzFile = 'results-%d.tgz'%(i+1)
    #
    #         cmd2 = "scp -i %s %s@%s:%s/../%s %s/"%(
    #                                 conf['rsaKeyFile'],conf['ssh_username'],host,
    #                                 conf['exp_dir'],tgzFile,conf['allResultsDir'])
    #         cmd3 = "cd %s; tar --overwrite --overwrite-dir -xf %s"%(conf['allResultsDir'],tgzFile)
    #
    #         cmd4 = "cd %s; cp %s /tmp; rm %s"%(conf['allResultsDir'],tgzFile,tgzFile)
    #         #run_cmd(cmd, True)
    #
    #         commands.append({'host':host,'cmds':[cmd2,cmd3,cmd4]})
    #
    #         print "Commands %s"%([cmd2,cmd3,cmd4])
    #
    #     runCommandsParallelForHost(commands, conf, True,local=True)
    
def start_backend(conf):
    print 'Starting minio and zookeeper...'

    hostCmds = []
    for idx,mac in enumerate(get_backends_list(conf)):
        outFile = '%s/backend_%02d.out'%(conf['out_dir'], idx)

        cmd = ( 'source ~/.bash_profile; '
                ' cd /tmp/kawkab; '
                ' %s/minio/reset.sh '
                ' > %s 2>&1 & ')%( conf['kawkab_dir'], outFile )

        hostCmds.append({'host':mac,'cmds':[cmd]})
    runCommandsParallelForHost(hostCmds, conf, True)

def start_servers(conf):
    print 'Starting servers...'

    jvmflags = conf['server_jvm_params']

    cp = ".:%s:%s/bin:%s"%(conf['kawkab_dir'],conf['kawkab_dir'],conf['server_classpath'])

    hostCmds = []
    commands = []
    for sidx, svr in enumerate(get_servers_list(conf)):

        outFile = '%s/server_%02d.out'%(conf['servers_dir'], sidx)

        #sport = conf['server_base_port'] + sidx

        sid = sidx

        cmd = ('source ~/.bash_profile; '
               ' cd /tmp; '
               ' java %s '
               ' -DnodeID=%d '
               ' -DoutFolder=%s ' # Used in kawkab.fs.core.PartitionedBufferedCache class
               ' -cp %s '
               #' kawkab.fs.cli.CLI '
               ' kawkab.fs.Main '
               ' > %s 2>&1 & ')%( jvmflags, sid, conf['servers_dir'], cp,
                                  outFile
                                  )

        #run_cluster_cmd(cmd, clMachName, conf, True)
        hostCmds.append({'host':svr,'cmds':[cmd]})
    #runCommandsParallelForHost([{'host':clMachName,'cmds':commands}], conf, True)
    runCommandsParallelForHost(hostCmds, conf, True)

    print 'Waiting for servers to start completely ...'
    stopwatch(25)

def start_clients(conf):
    print 'Starting clients...'

    jvmflags = conf['client_jvm_params']

    p = conf['test_params']
    wtMs = 0
    mport = conf['client_base_port']

    opts = ' mport=%d wt=%d nc=%d bs=%d rs=%d nf=%d fp=%s typ=%s tc=%d td=%d wr=%d iat=%f wmup=%d '%(
        mport, wtMs, p['nc'], p['bs'], p['rs'], p['fpc'], conf['run_dir'], p['typ'], p['tc'], p['td'], p['wr'], p['iat'], p['wmup'])
    cp = ".:%s:%s/bin:%s"%(conf['kawkab_dir'],conf['kawkab_dir'],conf['client_classpath'])

    now = time.time()

    conf['clientMachines'] = Set([])
    cpm = conf['clients_per_machine'] #clients per machine
    clCount = 0

    svrIdx = -1
    cid = 0
    mip = ""
    mid = 1
    servers = get_servers_list(conf)
    numServers = len(servers)
    hostCmds = []
    for i,clm in enumerate(get_clients_list(conf)):
        cid = i*p['nc'] + 1
        clCount += 1
        sip = servers[cid % numServers]

        #waitT = round((startTime - time.time())*1000)

        outFile = '%s/client_%02d.out'%(conf['clients_dir'], cid)

        if cid == 1:
            mid   = cid
            mip   = clm

        sport = conf['server_base_port'] + (cid % numServers)

        cmd = ('source ~/.bash_profile; '
               ' cd /tmp/kawkab; '
               ' java %s '
               ' -cp %s ' 
               ' kawkab.fs.testclient.ClientMain %s '
               ' cid=%d mid=%d mip=%s sip=%s sport=%d '
               ' > %s 2>&1 & ')%( jvmflags, cp, opts,
                                  cid, mid, mip, sip, sport,
                                  outFile
                )

        run_cluster_cmd(cmd, [clm], conf, True)
        #hostCmds.append({'host':clm,'cmds':[cmd]})
    #runCommandsParallelForHost(hostCmds, conf, True)

    print ''
    elapsed = time.time() - now
    print 'Started %d client processes in %.3f seconds.'%(clCount,elapsed)

def kill_processes(conf):
    cmds = [
            'pkill -9 java',
            'pkill -f ClientMain',
            'pkill cli.sh',
            'pkill -9 -f Main',
            'pkill -f zookeeper',
            'pkill -f server',
            'pkill minio'
            ]

    for cmd in cmds:
        runCmdParallel(cmd, "all", conf, True, True)
    
def cleanup(conf):
    print '-------'
    print 'Cleanup'
    print '-------'
    
    kill_processes(conf)
    
    cmd2 = 'rm -r %s/*'%(conf['run_dir'])
    runCmdParallel(cmd2, "all", conf, True)

    cmd2 = 'rm -r %s/fs0/fs/* %s/fs1/fs/* %s/fs/*'%(conf['kawkab_dir'], conf['kawkab_dir'], conf['kawkab_dir'])
    runCmdParallel(cmd2, "servers", conf, True)

    cmd2 = 'rm -r /tmp/fs0/fs/* /tmp/fs1/fs/* /tmp/fs/*'
    runCmdParallel(cmd2, "servers", conf, True)
    
    cmd = 'rm -r %s/*'%(conf['test_dir'])
    run_cmd(cmd, True)

def get_results(conf):
    p = conf['test_params']
    cmd = "echo %d, %d, %d, %d, %d, %d, %s, %.2f, %d, $(tail -n 1 %s/all-results.csv) >> %s/all-results.csv"%(
        p['wr'], p['tc'], p['nc'], p['fpc'], p['rs'], p['bs'], p['test_prefix'], p['iat'], p['test_run'],
        conf['test_dir'], conf['exp_dir'])
    run_cmd(cmd)

    print '============================================'
    cmd = "cat %s/all-results.json"%(conf['test_dir'])
    run_cmd(cmd)
    print '============================================'
    cmd = "cat %s/all-results.csv"%(conf['test_dir'])
    run_cmd(cmd)
    print '============================================'

def get_total_clients(conf):
    return len(get_clients_list(conf))
    
def run_batch(conf):
    res = ''
    run_n = 0
    run_t = len(conf['test_type'])*len(conf['test_prefix'])
    run_t *= len(conf['record_size'])
    run_t *= len(conf['files_per_client'])
    l = 0
    for _,_, iats in conf['batch_writeratio_rps']:
        l += len(iats)
    run_t *= l * len(conf['test_runs'])

    for test_type in conf['test_type']:
        for batch_size, write_ratio, iats in conf['batch_writeratio_rps']:
            for nf in conf['files_per_client']:
                for test_prefix in conf['test_prefix']:
                    #for batch_size in conf['batch_size']:
                    for record_size in conf['record_size']:
                        for iat in iats:
                            for test_run in conf['test_runs']:
                                #clients = degree*sl_size
                                numClients = get_total_clients(conf) * conf['clients_per_machine']
                                conf['test_id'] = '%s-%s-nc%d-bs%d-rs%d-nf%d-wr%d-iat%g'%(
                                                                                        test_type,
                                                                                        test_prefix,
                                                                                        numClients,
                                                                                        batch_size,
                                                                                        record_size,
                                                                                        nf,
                                                                                        write_ratio,
                                                                                        iat,
                                                                                    )
                                conf['test_dir'] = '%s/%s/run_%d'%(conf['exp_dir'],conf['test_id'],test_run)
                                conf['run_dir' ] = '/tmp/kawkab'

                                conf['test_params'] = {
                                    'typ'   : test_type,
                                    'wr'    : write_ratio,
                                    'fpc'    : nf,
                                    'test_prefix'   : test_prefix,
                                    'bs'    : batch_size,
                                    'rs'    : record_size,
                                    'iat'   : iat,
                                    'test_run'   : test_run,
                                    'nc'    : conf['clients_per_machine'],
                                    'tc'    : numClients,
                                    'td'    : conf['test_duration'],
                                    'wmup'  : conf['warmup_sec'],
                                }

                                #---------------------------------------------------

                                run_n += 1
                                print '-------------------------------------'
                                print 'Experiment %d of %d'%(run_n, run_t)
                                print 'Test ID: %s, Run=%d'%(conf['test_id'], test_run)
                                print '-------------------------------------'

                                #---------------------------------------------------

                                cleanup(conf)
                                stopwatch(3)
                                prepare_folders(conf)
                                start_backend(conf)
                                stopwatch(2)
                                start_servers(conf)
                                start_clients(conf)
                                stopwatch(3)

                                duration = conf['warmup_sec'] + conf['test_duration'] + 60
                                wait_for_process(conf, "clients", 'java', duration)
                                wait_for_process(conf, "servers", 'java', 5, True)

                                #---------------------------------------------------

                                time.sleep(3)

                                try:
                                    copy_files(conf)
                                except IOError as e:
                                    print "I/O error({0}): {1}".format(e.errno, e.strerror)

                                get_results(conf)

                                time.sleep(3)

    kill_processes(conf)
    print 'Finished...'

if __name__ == '__main__':
    conf = get_config()
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "stop":
            kill_processes(conf)
        else:
            print "Invalid argument: "+sys.argv[1]
        exit()
    run_batch(conf)
