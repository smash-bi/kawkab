#!/usr/bin/python

import threading
import sys
from utils import run_cmd

def config(conf):
    conf['targets'] = [
        "99.79.42.113",
        "15.222.244.8",
        "15.222.1.237",
        "35.183.99.247",
        "3.97.8.160",
        "3.96.145.103",

        "3.96.139.182",
        "3.96.189.72"
          ]
    
    #conf['src']      = "/home/sm3rizvi/canopus/lotkeeper/lot.tgz"
    #conf['src']      = "/home/sm3rizvi/canopus/ec2script.sh"
    conf['src']      = "/home/sm3rizvi/canopus/kawkab/lot/src"
    
    conf['dst']      = "/home/ubuntu/kawkab/"
    #conf['dst']      = "/home/sm3rizvi/tmp"
    
    conf['ssh_username'] = "ubuntu"
    conf['rsaKeyFile']  = "/home/sm3rizvi/canopus/rsa-keys/aws-keypair"
    conf['cmd'] = 'grep Exc /tmp/zoobench/servers/out/*/*'
    
    conf['makeCmd'] = "cd /home/ubuntu/kawkab; mvn compile"
    

def _runThreads(conf, hosts, func, funcArgs):
    """
    hosts is a list of IPs.
    """
    
    threads = []
    ret = []
    for host in hosts:
        try:
            d = {'host':host}
            args = (d,host) + funcArgs
            t = threading.Thread(target=func, args=args)
            threads.append(t)
            ret.append(d)
            t.start()
        except Exception as e:
            print('ERROR: unable to run thread for host',host)
            print(e)
    
    for i,t in enumerate(threads):
        t.join()
        out,err = ret[i]['out'],ret[i]['err']
        if out: print('\t',ret[i]['host'],'\n==============\n',out,'\n---------------')
        if err: print('\t',ret[i]['host'],'\n==============\n',err,'\n---------------')

def _copyFiles(returnDict,machine,username, src, dst, keyFile=None, download=False):
    identity = ""
    if keyFile:
        identity = ' -e "ssh -l %s -i %s -o StrictHostKeyChecking=no " '%(username,keyFile)
    cmd = 'rsync  -iprl %s %s %s@%s:%s | grep ".sT" '%(identity,src,username,machine,dst)

    if download:
        cmd = 'rsync  -zaiprl %s %s@%s:%s %s | grep ".sT" '%(identity,username,machine,src,dst)

    out,err = run_cmd(cmd, False)
    returnDict['out'] = out
    returnDict['err'] = err

def _runCmd(returnDict,machine,username,keyFile,cmnd,verbose=False):
    identity = ""
    if keyFile:
        identity = ' -i %s '%(keyFile)
    cmd = 'ssh -4 %s -oStrictHostKeyChecking=no %s@%s "%s" '%(identity,username,machine,cmnd)
    out,err = run_cmd(cmd, verbose)
    returnDict['out'] = out
    returnDict['err'] = err
    
def runCmd(conf, cmd=None):
    print('Running command...')
    #cmd = "cd /home/ubuntu; chmod u+x ec2script.sh; ./ec2script.sh"
    #cmd = "cd /home/ubuntu/lot/src/scripts; ./make.sh"
    if not cmd:
        cmd = conf['cmd']
        
    _runThreads(conf,conf['targets'],_runCmd,(conf['ssh_username'],conf['rsaKeyFile'],cmd))
    
def make(conf):
    print('Running make command...')
    cmd = conf['makeCmd']
    _runThreads(conf,conf['targets'],_runCmd,(conf['ssh_username'],conf['rsaKeyFile'],cmd))

def copyFiles(conf,src=None,dst=None):
    print('Copying files...')
    if not src: src = conf['src']
    if not dst: dst = conf['dst']

    print("Copying",src,"to",dst)

    _runThreads(conf,conf['targets'],_copyFiles, (conf['ssh_username'],src,dst,conf['rsaKeyFile']))

def downloadFiles(conf,src,dst):
    print('Downloading files...')

    print("Downloading",src,"to",dst)

    _runThreads(conf,conf['targets'],_copyFiles, (conf['ssh_username'],src,dst,conf['rsaKeyFile'], True))

def dowloadResults(conf):
    src = ""
    dst = ""


    
def sleep(conf):
    print('Sleeping')
    cmd = " sleep 100d "
    _runThreads(conf,conf['targets'],_runCmd,(conf['ssh_username'],conf['rsaKeyFile'],cmd,True))

def main():
    conf={}
    config(conf)
    
    copyFiles(conf)
    runCmd(conf)

if __name__ == '__main__':
    help = 'usage: '+sys.argv[0]+' sync | make | cmd | sleep | rsync <src> <dst> | dlres <src> <dst>'
    
    if len(sys.argv) <= 1:
        #main()
        print(help)
        sys.exit(0)
    
    if len(sys.argv) >= 2:
        type = sys.argv[1]
        conf={}
        config(conf)
        if type == 'sync':
            copyFiles(conf)
        
        elif type == 'make':
            make(conf)
        
        elif type == 'cmd':
            if len(sys.argv) > 3:
                print('usage: %s cmd "command"'%sys.argv[0])
                sys.exit(1)
            if len(sys.argv) == 2:
                runCmd(conf)
            else:
                runCmd(conf, sys.argv[2])
        
        elif type == 'sleep':
            sleep(conf)
        
        elif type == "rsync":
            if len(sys.argv) != 4:
                print('usage: %s rsync <src> <dst>'%sys.argv[0])
                sys.exit(1)
            src = sys.argv[2]
            dst = sys.argv[3]
            copyFiles(conf,src,dst)

        elif type == "dlres":
            if len(sys.argv) != 4:
                print('usage: %s dlres <src> <dst>'%sys.argv[0])
                sys.exit(1)
            src = sys.argv[2]
            dst = sys.argv[3]
            downloadFiles(conf, src, dst)

        elif type == 'help':
            print(help)
        
        else:
            print('Unknown argument:',type)
            print(help)
        
        sys.exit(0)
    
    print('invalid parameters')
    print(help)
    print(sys.argv)