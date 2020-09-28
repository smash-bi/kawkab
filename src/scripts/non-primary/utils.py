import subprocess as sp
import os
import time
import sys
#import threading
from threading import Lock,Thread

lock = Lock()

def create_file(file_name, data, host=None, conf=None):
    print 'Creating file: %s'%file_name

    if host:
        cmd = 'echo \'%s\' > %s'%(data, file_name)
        runCmdParallel(cmd, host, conf, False)
    else:
        file = open(file_name, 'w')
        file.write(data)
        file.close()
        
def read_file(file_name):
    print 'Reading file: %s'%file_name
    file = open(file_name, 'r')
    lines = file.readlines()
    file.close()
    
    return lines

def make_dirs(dir, hosts=None, conf=None):
    '''
        hosts is a string which  represents the subset of machines to run:
            all: all clients and servers
            servers: run on all the servers
            clients: run only on the clients
            
        if hosts is None, the command is run locally
        
        conf is required if hosts is given
    '''
    #if hosts is None:
    #    if os.path.exists(dir): return
    #    os.makedirs(dir, 0x1EC)
    #    return
    
    cmd = 'if [ ! -d "%s" ]; then mkdir -p %s; fi'%(dir, dir)
    
    if hosts:
        #run_cluster_cmd(cmd, hosts, conf, True)
        runCmdParallel(cmd, hosts, conf, True)
    else:
        run_cmd(cmd, True)

def get_backends_list(conf):
    machines = []
    for rack in conf['backend_indexes']:
        ip_base = conf['cluster_ips'][rack[0]]
        for icl in rack[1]:
            machines.append("%s.%d"%(ip_base, icl))

    return machines

def get_servers_list(conf):
    machines = []
    for rack in conf['server_indexes']:
        ip_base = conf['cluster_ips'][rack[0]]
        for icl in rack[1]:
            machines.append("%s.%d"%(ip_base, icl))

    return machines

def get_clients_list(conf):
    machines = []
    for rack in conf['client_indexes']:
        ip_base = conf['cluster_ips'][rack[0]]
        for icl in rack[1]:
            machines.append("%s.%d"%(ip_base, icl))

    return machines


def get_hosts_list(conf, hosts):
    '''
        hosts can be one of: pool, all, servers, clients, hostname/ip, [list of IPs/hostnames]
    '''
    
    hosts_list = []
    if hosts == "all":
        hosts_list = set(get_servers_list(conf))
        hosts_list = hosts_list.union(set(get_clients_list(conf)))
        hosts_list = hosts_list.union(set(get_backends_list(conf)))
        #hosts_list = conf['servers_pool_names'] + conf['clients_pool_names']
        _print("all = %s"%str(hosts_list))
    elif hosts == "servers":
        hosts_list = get_servers_list(conf)
    elif hosts == "clients":
        hosts_list = get_clients_list(conf)
    elif hosts == "backends":
        hosts_list = get_backends_list(conf)
    elif type(hosts) == list:
        hosts_list = hosts
    elif hosts is not None:
        hosts_list = [hosts]
    else:
        hosts_list = []
        
    return hosts_list

def run_cmd(cmd,verbose=True,showErr=True):
    if verbose: _print( 'cmd: '+cmd)
    
    process = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
    out,err = process.communicate()
    
    if out:
        out=out.strip()
        if verbose: _print('Output: ' + out.strip())
    if err:
        err=err.strip();
        if showErr: _print('ERROR: ' + err.strip())
    return out,err

def run_cluster_cmd(cmd, hosts, conf,verbose=True,isRoot=False):
    '''
        hosts can be one of: pool, all, servers, clients, hostname/ip, [list of IPs/hostnames]
    '''
    hosts_list = get_hosts_list(conf, hosts)
    
    keyFile = "" if conf['rsaKeyFile']==None else " -i %s "%conf['rsaKeyFile']
    username = conf['ssh_username']
     
    outs = {}
    errs = {}
    for node in hosts_list:
        sudo = "sudo " if isRoot else ""
        
        if conf['run_local']:
            out,err = run_cmd(cmd,verbose)
        #elif isRoot:
            #out, err = run_cmd('ssh %s -oStrictHostKeyChecking=no %s "sudo %s"'%(keyFile,node,cmd),verbose)
        else:
            out, err = run_cmd('ssh %s -oStrictHostKeyChecking=no %s@%s "%s %s"'%(keyFile,username,node, sudo,cmd),verbose)
        
        outs[node] = out
        errs[node] = err
    
    return outs,errs

def _runThreads(conf, hosts, func, funcArgs):
    """
    hosts is a list of IPs.
    """
    
    threads = []
    ret = []
    outs = {}
    errs = {}
    
    for host in hosts:
        try:
            d = {'host':host}
            args = (d,host) + funcArgs
            t = Thread(target=func, args=args)
            threads.append(t)
            ret.append(d)
            t.start()
        except Exception as e:
            _print('ERROR: unable to run thread for host '+host)
            _print(e)
    
    for i,t in enumerate(threads):
        t.join()
        out,err = ret[i]['out'],ret[i]['err']
        outs[ret[i]['host']] = out
        errs[ret[i]['host']] = err
        if out: _print('\t%s\n==============\n%s\n---------------'%(ret[i]['host'],out))
        if err: _print('\t%s\n==============\n%s\n---------------'%(ret[i]['host'],err))
    
    return outs,errs


def runCommandsParallel(dicts,conf,verbose=True,isRoot=False):
    """
    dicts is list of dictionaries:  [{'cmd':cmd, 'host':host}]
    """
    
    username = conf['ssh_username']
    keyFile = "" if conf['rsaKeyFile']==None else conf['rsaKeyFile']
    
    threads = []
    ret = []
    outs = {}
    errs = {}
    
    for dict in dicts:
        try:
            host = dict['host']
            cmd  = dict['cmd']
            
            retD = {'host':host}
            args = (retD,host,username,keyFile,cmd,verbose,isRoot)
            t = Thread(target=_runCmd, args=args)
            threads.append(t)
            ret.append(retD)
            t.start()
        except Exception as e:
            _print('ERROR: unable to run thread for host '+host)
            _print(e)
    
    for i,t in enumerate(threads):
        t.join()
        out,err = ret[i]['out'],ret[i]['err']
        outs[ret[i]['host']] = out
        errs[ret[i]['host']] = err
        if out: _print('\t%s\n==============\n%s\n---------------'%(ret[i]['host'],out))
        if err: _print('\t%s\n==============\n%s\n---------------'%(ret[i]['host'],err))
    
    return outs,errs

def runCommandsParallelForHost(dicts,conf,verbose=True,isRoot=False,local=False):
    """
    dicts is a list of dictionary: dicts = [{'host':host,'cmds':[cmd]}]
    """
    
    username = conf['ssh_username']
    keyFile = "" if conf['rsaKeyFile']==None else conf['rsaKeyFile']
    
    threads = []
    ret = []
    outs = {}
    errs = {}
    
    for dict in dicts:
        try:
            host = dict['host']
            cmd  = dict['cmds']
            
            retD = {'host':host}
            args = (retD,host,username,keyFile,cmd,verbose,isRoot,local)
            t = Thread(target=_runCmds, args=args)
            threads.append(t)
            ret.append(retD)
            t.start()
        except Exception as e:
            _print('ERROR: unable to run thread for host '+host)
            _print(e)
    
    for i,t in enumerate(threads):
        t.join()
        out,err = ret[i]['out'],ret[i]['err']
        outs[ret[i]['host']] = out
        errs[ret[i]['host']] = err
        if out: _print('\t%s\n==============\n%s\n---------------'%(ret[i]['host'],out))
        if err: _print('\t%s\n==============\n%s\n---------------'%(ret[i]['host'],err))
    
    return outs,errs


def _runCmd(returnDict,machine,username,keyFile,cmnd,verbose=True,isRoot=False,local=False):
    identity = ""
    if not local and keyFile and len(keyFile) > 0:
        identity = ' -i %s '%(keyFile)
    
    sudo = " sudo " if isRoot else ""
    
    if local:
        cmd = '%s %s'%(sudo,cmnd)
    else:
        cmd = 'ssh -4 %s -oStrictHostKeyChecking=no %s@%s "%s %s" '%(identity,username,machine,sudo,cmnd)
    #_print("\t\tCmd:")
    out,err = run_cmd(cmd, verbose)
    returnDict['out'] = out
    returnDict['err'] = err
    
def _runCmds(returnDict,machine,username,keyFile,cmnds,verbose=True,isRoot=False,local=False):
    identity = ""
    if keyFile and len(keyFile) > 0:
        identity = ' -i %s '%(keyFile)
    
    outs = []
    errs = []
    
    for cmnd in cmnds:
        sudo = " sudo " if isRoot else ""
        if local:
            cmd = '%s %s '%(sudo,cmnd)
        else:  
            cmd = 'ssh -4 %s -oStrictHostKeyChecking=no %s@%s "%s %s" '%(identity,username,machine,sudo,cmnd)
        #_print("\t\tCmd:")
        out,err = run_cmd(cmd, verbose)
        
        outs.append(out)
        errs.append(err)
        
    returnDict['out'] = outs
    returnDict['err'] = errs
    
def runCmdParallel(cmd, hosts, conf, verbose=True, isRoot=False):
    '''
        hosts can be one of: pool, all, servers, clients, hostname/ip, [list of IPs/hostnames]
    '''
    hosts_list = get_hosts_list(conf, hosts)
    username = conf['ssh_username']
    keyFile = "" if conf['rsaKeyFile']==None else conf['rsaKeyFile']
    
    _runThreads(conf,hosts_list,_runCmd,(username,keyFile,cmd,verbose,isRoot))
    
def wait_for_process(conf, hosts, pname, limit=600, kill=False):
    '''
        hosts can be one of: pool, all, servers, clients, hostname/ip, [list of IPs/hostnames]
    '''
    
    keyFile = "" if conf['rsaKeyFile']==None else " -i %s "%conf['rsaKeyFile']
    username = conf['ssh_username']

    if kill:
        cmd = 'pkill -f %s'%pname
        run_cluster_cmd(cmd, hosts, conf)
    
    if conf['onEC2']:
        cmd = 'pgrep -xl %s'%(pname)
    else:
        cmd = 'pgrep -fl %s | grep java'%(pname)

    cnt = 1
    hosts_list = get_hosts_list(conf, hosts)
    for host in hosts_list:
        _print('Waiting for process %s to finish on %s'%(pname, host))

        while cnt <= limit:
            if not conf['run_local']:
                cmd1 = 'ssh %s -oStrictHostKeyChecking=no %s@%s "%s"'%(keyFile,username, host,cmd)
            
            out,err = run_cmd(cmd1, False)
            if err and len(err) > 0:
                _print(err)
                break
            if out and len(out) > 0:
                print " %d "%cnt,
                sys.stdout.flush()
                time.sleep(2)
                cnt += 1
            else:
                break
   
def stopwatch(t):
    while t > 0:
        print '%3d            \r'%t,
        t -= 1
        sys.stdout.flush()
        time.sleep(1)
    print '\r\n'

def get_server(conf, iSvr):
    ip = conf['servers_pool'][iSvr]
    port = conf['lot_server_base_port']+iSvr
    return (ip,port)

def get_host_info(conf):
    info = {}
    info['hostname'],_ = run_cmd('hostname')
    info['nic_iface'] = 'p6p1' if conf['my_hostname'][-2:]=='00' else 'p4p1'
    info['nic_ip'] = conf['cluster']['my_hostname']
    nic_ip = info['nit_ip'].split(".")
    info['nic_subnet'] = '%s.%s.0/24'%(nic_ip[0],nic_ip[1])
    return info

def get_local_ips(conf):
    
    localIPs = {}
    commands = []
    
    for host in conf['all_machines']:
        iface = conf['iface']
        if not conf['onEC2'] and host[-2:] == '00':
            iface = 'p6p1'
        
        cmd = "/sbin/ifconfig %s | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*'"%(iface)
            
        #localIPs[host] = get_local_ip(conf, host, iface)
        commands.append({'host':host,'cmd':cmd})
    
    outs,errs = runCommandsParallel(commands, conf, True, False)
    
    for host in conf['all_machines']:
        localIPs[host] = outs[host]
        
    _print('localIPS: %s'%(localIPs))
    return localIPs
    
def get_local_ip(conf, host, iface):
    cmd = "/sbin/ifconfig %s | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*'"%(iface)
    out,err = run_cluster_cmd(cmd, [host], conf)
    
    return out[host]
    

def _print(msg):
    with lock:
        print msg
        
def _read_lines(fname):
    try:
        f = open(fname)
    except IOError as e:
        _print("I/O error({0}): {1}".format(e.errno, e.strerror))
        _print("file: "+fname)
        raise e
    
    lines = f.readlines()
    f.close()
    return lines

def read_results_json(fname, params):
    lines = _read_lines(fname)
    
    lines = "".join(lines)
    lines = lines.replace(",\n}","\n}").replace("], ]", "]]").replace("%!f(int64=","").replace(")","")
    data = json.loads(lines)
    
    if params["test_types"].startswith("ep"):
        data = np.flipud(data)
    
    return data

