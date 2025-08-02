import os
import subprocess
from subprocess import PIPE, Popen
import time
import datetime
import ruamel.yaml
from IPython import embed
from termcolor import colored
import argparse
import random

TTCS_COORDINATOR_IP="34.73.25.115"
UGIA_IP=""
# IMAGE_FAMILY="tiga-img"
IMAGE_FAMILY="tiga-img-ubuntu16"
LOGIN_PATH = "/home/steam"
CONFIG_PATH= f"{LOGIN_PATH}/Tiga"
BAZEL_BIN_PATH = f"{LOGIN_PATH}/Tiga/bazel-bin/"
JANUS_BIN_PATH= f"{LOGIN_PATH}/Tiga/janus/build/"
LOG_PATH = f"{LOGIN_PATH}/Tiga/scripts/log/"

NCC_BIN_PATH=f"{LOGIN_PATH}/Tiga/ncc/janus/build/"
COLLECT_LOG = False
LOG_FOLDER=f"{LOGIN_PATH}/Tiga/scripts/log/"
CHECK_POINT_FILE = f"{LOGIN_PATH}/tiga.ckpt"
STATS_PATH = f"/mnt/disks/data"
LOCAL_CLEAN_FILE=f"{LOGIN_PATH}/Tiga/scripts/clean.sh"
TEST_PLAN_FILE=f"{LOGIN_PATH}/Tiga/scripts/test_plan.yaml"


SUMMARY_STATS_PATH=f"{LOGIN_PATH}/Tiga/scripts/summary/"
FIGS_PATH=f"{LOGIN_PATH}/Tiga/scripts/figs/"

TAG = "uiga"
SSH_KEY = f"{LOGIN_PATH}/.ssh/ae_rsa"
ssh_identity = '-i {}'.format(SSH_KEY) if SSH_KEY else ''
# Prefix for SSH and SCP.
SSH = 'ssh {} -q -o ConnectTimeout=2 -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no '.format(
    ssh_identity)
SCP = 'scp -r {} -q -o ConnectTimeout=2 -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no '.format(
    ssh_identity)
USERNAME = "steam"
CMD_RETRY_TIMES = 3


###########################################################
SERVER_VM_TYPE = "n2-standard-16"
PROXY_VM_TYPE = "n2-standard-16"
SERVER_REGIONS = ["us-east1-b", "europe-north1-a", "southamerica-east1-a"] 
# proxies are distributed across SERVER_REGIONS and REMOTE_REGIONS
REMOTE_REGIONS = ["asia-east2-b"] 
ALL_REGIONS = SERVER_REGIONS + REMOTE_REGIONS

SHUTDOWN_AFTER_RUN = True

############################################################
TIGA_BOUND_CAP = 400000
TIGA_INIT_BOUND_ESTIMATION_LOCAL =[60000, 100000, 100000]  
CALVIN_INIT_BOUND_ESTIMATION = [60000, 100000, 100000]
TIGA_INIT_BOUND_ESTIMATION_REMOTE = 150000
LOG_UNIT = 10000
CALVIN_SEQUENCER_MAP_TPCC={
    0: [0,0], # [shardId, replicaId]
    1: [1,1],
    2: [2,2],
    3: [3,0],
    4: [4,1],
    5: [5,2],
    6: [-1,-1],
    7: [-1,-1],
}
CALVIN_SEQUENCER_MAP_MICRO ={
    0: [0,0], # [shardId, replicaId]
    1: [1,1],
    2: [2,2],
    3: [0,0],
    4: [1,1],
    5: [2,2],
    6: [-1,-1],
    7: [-1,-1],
}
REGION_PROXIES = [
    [0,3],
    [1,4],
    [2,5],
    [6,7]
]
PROXY_REGIONS = [
    0,1,2,0,1,2,3,3
]

TIGA_CONFIG_MODES = {
    "2pl": {"cc":"2pl_ww", "ab": "multi_paxos"},
    "occ": {"cc":"occ", "ab": "multi_paxos"},
    "tapir": {"cc":"tapir", "ab": "tapir"},
    "janus": {"cc":"brq", "ab": "brq"},
    "calvin": {"cc":"calvin", "ab": "calvin"},
    "detock": {"cc":"detock", "ab": "detock"},
    "tiga": {"cc":"tiga", "ab": "tiga"},
    "ncc": {"cc":"acc", "ab": "acc"},
    "ncc-ft":{"cc":"acc", "ab": "acc"},
}


############################################################

def scp_files(server_ip_list, local_path_to_file, remote_dir, to_remote):
    # return
    '''
    copies the file in 'local_path_to_file' to the 'remote_dir' in all servers
    whose external ip addresses are in 'server_ip_list'

    args
        server_ip_list: list of external IP addresses to communicate with
        local_path_to_file: e.g. ./script.py
        remote_dir: e.g. ~
        to_remote: whether to copy to remote (true) or vice versa (false)
    returns
        boolean whether operation was succesful on all servers or not
    '''
    src = remote_dir if not to_remote else local_path_to_file
    src_loc = 'remote' if not to_remote else 'local'
    dst = remote_dir if to_remote else local_path_to_file
    dst_loc = 'remote' if to_remote else 'local'

    message = 'from ({src_loc}) {src} to ({dst_loc}) {dst}'.format(
        src_loc=src_loc, src=src, dst_loc=dst_loc, dst=dst)
    print('---- started scp {}'.format(message))

    procs = []
    for server in server_ip_list:
        if to_remote:
            cmd = '{} {} {}@{}:{}'.format(SCP, local_path_to_file,
                                          USERNAME, server, remote_dir)
            proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
        else:
            cmd = '{} {}@{}:{} {}'.format(SCP, USERNAME, server,
                                          remote_dir, local_path_to_file)
            proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
        # print("scp cmd ", cmd)
        procs.append((server, proc, cmd))

    success = True
    procs_error = retry_proc_error(procs)
    retries = 1
    while retries < CMD_RETRY_TIMES and procs_error:
        procs_error = retry_proc_error(procs)
        retries += 1

    if retries >= CMD_RETRY_TIMES and procs_error:
        success = False
        for server, proc, cmd in procs_error:
            output, err = proc.communicate()
            if proc.returncode != 0:
                print(
                    colored('[{}]: FAIL SCP - [{}]'.format(server, cmd),
                            'yellow'))
                print(colored('Error Response:', 'blue', attrs=['bold']),
                      proc.returncode, output, err)

    if success:
        print(
            colored('---- SUCCESS SCP {} on {}'.format(message,
                                                       str(server_ip_list)),
                    'green',
                    attrs=['bold']))
    else:
        print(
            colored('---- FAIL SCP {}'.format(message), 'red', attrs=['bold']))
    return success


def retry_proc_error(procs_list):
    procs_error = []
    for server, proc, cmd in procs_list:
        output, err = proc.communicate()
        if proc.returncode != 0:
            proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
            procs_error.append((server, proc, cmd))
    return procs_error
    
def run_command(server_ip_list, cmd, in_background=True):
    # return
    '''
    runs the command 'cmd' in all servers whose external ip addresses are 
    in 'server_ip_list'

    cfg
        server_ip_list: list of external IP addresses to communicate with
        cmd: command to run
    returns
        boolean whether operation was succesful on all servers or not
    '''
    if not in_background:
        print('---- started to run command - [{}] on {}'.format(
            cmd, str(server_ip_list)))
    else:
        print(
            colored('---- started to run [IN BACKGROUND] command - [{}] on {}'.
                    format(cmd, str(server_ip_list)),
                    'blue',
                    attrs=['bold']))
    procs = []
    for server in server_ip_list:
        ssh_cmd = '{} {}@{} {}'.format(SSH, USERNAME, server, cmd)
        proc = Popen(ssh_cmd.split(), stdout=PIPE, stderr=PIPE)
        procs.append((server, proc, ssh_cmd))

    success = True
    output = ''
    if not in_background:
        procs_error = retry_proc_error(procs)
        retries = 1
        while retries < CMD_RETRY_TIMES and procs_error:
            procs_error = retry_proc_error(procs)
            retries += 1

        if retries >= CMD_RETRY_TIMES and procs_error:
            success = False
            for server, proc, cmd in procs_error:
                output, err = proc.communicate()
                if proc.returncode != 0:
                    print(
                        colored(
                            '[{}]: FAIL run command - [{}]'.format(
                                server, cmd), 'yellow'))
                    print(colored('Error Response:', 'blue', attrs=['bold']),
                          proc.returncode, output, err)

        if success:
            print(
                colored('---- SUCCESS run command - [{}] on {}'.format(
                    cmd, str(server_ip_list)),
                        'green',
                        attrs=['bold']))
        else:
            print(
                colored('---- FAIL run command - [{}]'.format(cmd),
                        'red',
                        attrs=['bold']))

    return success, output


def print_error(msg):
    print(colored(msg, "red", attrs=['bold']))

def print_info(msg):
    print(colored(msg, "yellow", attrs=['bold']))

def print_good(msg):
    print(colored(msg, "green", attrs=['bold']))

def get_key_value(d, key, default, print_msg = True):
    ret = d[key] if key in d else default 
    if print_msg:
        print_info(f"{key}:{ret}")
    return ret


def is_remote_proxy(idx):
    return idx >= 6

def get_remote_region_designate_idx(idx):
    return (idx - 6) % 3

def get_region_idx(idx):
    if idx >=6:
        return -1 
    return idx % 3