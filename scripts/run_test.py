import os
import subprocess
from subprocess import PIPE, Popen
import time
import tiga_common
import gcp_tools
import ruamel.yaml

scp_files = tiga_common.scp_files
run_command = tiga_common.run_command
print_error = tiga_common.print_error
print_info = tiga_common.print_info 
print_good = tiga_common.print_good 
get_key_value = tiga_common.get_key_value

def copy_binaries(vm_ips):
    scp_files(vm_ips, tiga_common.LOCAL_CLEAN_FILE, 
        "clean.sh", to_remote = True)

    # Copy Tiga's binaries
    binary_path = f"{tiga_common.BAZEL_BIN_PATH}/TigaEntity/"
    os.system(f"sudo chmod  777  {binary_path}/TigaServer")
    os.system(f"sudo chmod  777  {binary_path}/TigaClient")
    scp_files(vm_ips, f"{binary_path}/TigaServer", 
        "TigaServer", to_remote = True)
    scp_files(vm_ips, f"{binary_path}/TigaClient", 
        "TigaClient", to_remote = True)

    # Copy Calvin's binaries
    binary_path = f"{tiga_common.BAZEL_BIN_PATH}/CalvinEntity/"
    os.system(f"sudo chmod  777  {binary_path}/CalvinServer")
    os.system(f"sudo chmod  777  {binary_path}/CalvinClient")
    scp_files(vm_ips, f"{binary_path}/CalvinServer", 
            "CalvinServer", to_remote = True)
    scp_files(vm_ips, f"{binary_path}/CalvinClient", 
            "CalvinClient", to_remote = True)
    
    # Copy Detock's binaries
    binary_path = f"{tiga_common.BAZEL_BIN_PATH}/DetockEntity/"
    os.system(f"sudo chmod  777  {binary_path}/DetockServer")
    os.system(f"sudo chmod  777  {binary_path}/DetockClient")
    scp_files(vm_ips, f"{binary_path}/DetockServer", 
            "DetockServer", to_remote = True)
    scp_files(vm_ips, f"{binary_path}/DetockClient", 
            "DetockClient", to_remote = True)    
    
    # Copy Janus' binaries
    binary_path =  f"{tiga_common.JANUS_BIN_PATH}/"
    os.system(f"sudo chmod  777  {binary_path}/deptran_server")
    scp_files(vm_ips, f"{binary_path}/deptran_server", 
            "deptran_server", to_remote = True)

    # Copy NCC' binaries
    binary_path =  f"{tiga_common.NCC_BIN_PATH}/"
    os.system(f"sudo chmod  777  {binary_path}/deptran_server")
    scp_files(vm_ips, f"{binary_path}/deptran_server", 
            "deptran_server_ncc", to_remote = True)

    # Copy Paxos binaries: for NCC-FT
    binary_path = f"{tiga_common.BAZEL_BIN_PATH}/PaxosEntity/"
    os.system(f"sudo chmod  777  {binary_path}/PaxosServer")
    os.system(f"sudo chmod  777  {binary_path}/PaxosClient")
    scp_files(vm_ips, f"{binary_path}/PaxosServer", 
            "PaxosServer", to_remote = True)
    scp_files(vm_ips, f"{binary_path}/PaxosClient", 
            "PaxosClient", to_remote = True)    
    


if __name__ == '__main__':
    parser = tiga_common.argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--num_replicas',  type=int, default = 3,
                        help='Specify the number of replicas ')
    parser.add_argument('--num_shards',  type=int, default = 3,
                        help='Specify the number of shards ')
    parser.add_argument('--num_proxies',  type=int, default = 8,
                        help='Specify the number of proxies ')
    parser.add_argument('--test_plan',  type=str, default = tiga_common.TEST_PLAN_FILE,
                    help='Specify the path of the test_plan yaml file')
    parser.add_argument('--only_copy',  action='store_true',
                    help='Only copy the binaries/configs without execution')
    args = parser.parse_args()
    num_replicas = args.num_replicas
    num_shards = args.num_shards
    num_servers = num_replicas * num_shards
    num_proxies = args.num_proxies 
    test_plan_file = args.test_plan
    print("replicas: ", num_replicas)
    print("shards: ", num_shards)
    print("proxies: ", num_proxies)
    print("test plan file: ", test_plan_file)

    server_names, server_ips = gcp_tools.get_ips_by_name(
        cluster_name= tiga_common.TAG+"-server-", 
        zone=",".join(tiga_common.SERVER_REGIONS))
    proxy_names, proxy_ips = gcp_tools.get_ips_by_name(
        cluster_name=tiga_common.TAG+"-proxy-", 
        zone=",".join(tiga_common.ALL_REGIONS))
    server_name_dict = {}
    for i in range(len(server_names)):
        server_name_dict[server_names[i]] = server_ips[i]
    
    # restructure the servers' ips
    host_dict = {}
    process_dict = {}
    server_names = []
    server_ips = []
    for sid in range(num_shards):
        for rid in range(num_replicas):
            server_name =(tiga_common.TAG+"-server-s-"
                +str(sid).zfill(2)+"-r-"+str(rid).zfill(2))
            server_names.append(server_name)
            server_ips.append(server_name_dict[server_name])
            host_dict[server_name] = server_name_dict[server_name]
            process_dict[server_name] = server_name

    proxy_names, proxy_ips= proxy_names[0:num_proxies], proxy_ips[0:num_proxies]
    for i in range(len(proxy_names)):
        process_name = proxy_names[i]
        host_dict[process_name] = proxy_ips[i]

    vm_ips = server_ips + proxy_ips 
    vm_names = server_names + proxy_names

    # copy_binaries(vm_ips)
    run_command(vm_ips, "./clean.sh", in_background=False)

    yaml = ruamel.yaml.YAML()
    test_plan_f = open(test_plan_file, 'r')
    test_plan = yaml.load(test_plan_f)
    fpckpt = open(tiga_common.CHECK_POINT_FILE, "w")
    for case in test_plan:
        prefix = get_key_value(case, "prefix", "")
        test_no = get_key_value(case, "test_no", 0)
        bench_type = get_key_value(case, "bench_type", "Micro")
        rate = get_key_value(case, "rate", 1000)
        preventive = get_key_value(case, "preventive", True)
        test_type = get_key_value(case, "test_type", "tiga")
        leader_rotation = get_key_value(case, "leader_rotation", False)
        skew_factor = get_key_value(case, "skew_factor", 0.5)
        key_num = get_key_value(case, "key_num", 1000000)
        owd_delta_us = get_key_value(case, "owd_delta_us", 10000)
        owd_estimate_percentile = get_key_value(
            case, "owd_estimate_percentile", 50)
        test_failure_recovery = get_key_value(
            case, "test_failure_recovery", False)
        synced_logid_before_failure = get_key_value(
            case, "synced_logid_before_failure", 600000)
        clock_approach = get_key_value(
            case, "clock_approach", "cwcs")
        run_time_sec = get_key_value(
            case, "run_time_sec", 30
        )
        grace_period_sec = get_key_value(
            case, "grace_period_sec", 20)

        # Generate configs
        if bench_type == "TPCC":
            config_template = f"{tiga_common.CONFIG_PATH}/config-local-tpcc.yml"
            state_machine_info = ""
        elif bench_type == "Micro":
            config_template = f"{tiga_common.CONFIG_PATH}/config-local-micro.yml"
            skew=int(skew_factor*100)
            state_machine_info = f"zipfian-{skew}-{key_num}"
        else:
            print("not implemented benchtype ", bench_type)
            exit(0)

        preventive_mark = "preventive" if preventive else "detective"
        leader_mark = "leader-sep" if leader_rotation else "leader-colo" 
        case_name = (f"{prefix}{test_no}-{test_type}-{leader_mark}"
                    f"-{preventive_mark}-{bench_type}-{state_machine_info}"
                    f"-S-{num_shards}-R-{num_replicas}-P-{num_proxies}-Rate-{rate}"
                    f"-OWD-{owd_estimate_percentile}p-{owd_delta_us}usCap")
        print_info(case_name)

        start_time = time.time()
        fpckpt.write(f"Start\t{case_name}-{start_time}\n")
        fpckpt.flush()

        config_file =  f"{tiga_common.CONFIG_PATH}/config-tpl-n.yml"
        f = open(config_template, "r")
        yaml_data = yaml.load(f)
        yaml_data["mode"]["ab"] = tiga_common.TIGA_CONFIG_MODES[test_type]["ab"]
        yaml_data["mode"]["cc"] = tiga_common.TIGA_CONFIG_MODES[test_type]["cc"] 
        yaml_data["bench"]["coefficient"] = skew_factor
        yaml_data["client"] = {} 
        yaml_data["client"]["type"]="open"
        yaml_data["client"]["rate"]=rate
        yaml_data["client"]["max_undone"]= -1
        yaml_data["client"]["forwarding"]=False
        yaml_data["n_concurrent"] = 1
        yaml_data["server_initial_bound"] = tiga_common.CALVIN_INIT_BOUND_ESTIMATION
        yaml_data["server_bound_cap"] = [ 
            tiga_common.TIGA_BOUND_CAP for i in range(num_replicas) ] 
        yaml_data["designate_replica_id"] = [ 
            i % num_replicas for i in range(num_shards) ]
        yaml_data["test_failure_recovery"] = test_failure_recovery
        yaml_data["synced_logid_before_failure"] = synced_logid_before_failure
        yaml_data["clock_approach"] = clock_approach

        client_partitions = []
        client_groups = []
        for i in range(len(proxy_names)):
            proxy_name = proxy_names[i]
            client_groups.append(proxy_name)
            if len(client_groups) == num_replicas:
                client_partitions.append(client_groups)
                client_groups = []
            process_dict[proxy_name] = proxy_name

        if len(client_groups)>0:
            client_partitions.append(client_groups)

        yaml_data["site"]["client"] = client_partitions
        yaml_data["host"] = host_dict
        yaml_data["process"] = process_dict

        servers_list = []
        port_offset = 0
        for sid in range(num_shards):
            one_shard_group = []
            for rid in range(num_replicas):
                server_name = (tiga_common.TAG
                    +"-server-s-"+str(sid).zfill(2)
                    +"-r-"+str(rid).zfill(2))
                one_shard_group.append(server_name+":"+str(10000+port_offset))
                port_offset+= 100
            if leader_rotation:
                # separate leaders to different regions
                one_shard_group = one_shard_group[sid:] + one_shard_group[0:sid]
            servers_list.append(one_shard_group)
        yaml_data["site"]["server"] = servers_list 
        single_replica_list = []
        if test_type == 'ncc' or test_type == 'ncc-ft':
            yaml_data["site"]["replica"] = servers_list
            for shard_server in servers_list:
                single_replica_list.append(shard_server[0:1])
            yaml_data["site"]["server"] = single_replica_list 

        yaml_data["preventive"] = preventive
        yaml_data["owd_delta_us"] = owd_delta_us
        yaml_data["owd_estimate_percentile"] = owd_estimate_percentile
        out_file = open(config_file, "w")
        yaml.indent(sequence=4, offset=2)  
        yaml.dump(yaml_data, out_file)
        print("Yaml Dumped")

        os.system(f"sudo rm -rf {tiga_common.CONFIG_PATH}/*.yaml")
        run_command(vm_ips, "./clean.sh", in_background=False)
        time.sleep(5)
        # Copy config
        scp_files(server_ips+proxy_ips, 
                    f"{tiga_common.CONFIG_PATH}/config-tpl-n.yml",
                    "config-tpl-n.yml", to_remote = True)

        
        if args.only_copy is True:
            exit(0)

        if test_type not in tiga_common.TIGA_CONFIG_MODES.keys():
            print_error(f"Unsupported test_type {test_type}")
            ext(0)
        ## Launch replicas (id starts from 0)
        if test_type == "ncc-ft":
            # Each shard has its own replication layer
            for sid in range(num_shards):
                for rid in range(num_replicas):
                    server_name = (tiga_common.TAG+"-server-s-"
                        +str(sid).zfill(2)+"-r-"+str(rid).zfill(2))
                    log_file = "replica-log-s-"+str(sid)+"-r-"+str(rid)
                    replica_cmd = (f"GLOG_logtostderr=1 ./PaxosServer "
                        " --config=./config-tpl-n.yml "
                        f" --shardId={sid} --replicaId={rid} "
                        f" --shardNum={num_shards} --replicaNum={num_replicas} "
                        f" --serverName={server_name} "
                        f" --ioThreads=2 --workerNum=1 > {log_file} 2>&1 &")
                    print_info(f"{server_name_dict[server_name]}\t{replica_cmd}")
                    run_command([server_name_dict[server_name]], 
                        replica_cmd, in_background=False)
            time.sleep(5)

            for sid in range(num_shards):
                server_name =tiga_common.TAG+"-server-s-"+str(sid).zfill(2)+"-r-"+str(0).zfill(2)
                log_file =  "server-log-s-"+str(sid)+"-r-"+str(0)                                
                server_cmd = (f"./deptran_server_ncc -b  "
                    " -r \"\" "
                    f" -t {run_time_sec} "
                    f" -f ./config-tpl-n.yml -d 30 "
                    f" -P {server_name} > {log_file} 2>&1 &") 
                print_info(f"{server_name_dict[server_name]}\t{server_cmd}")
                run_command([server_name_dict[server_name]], 
                    server_cmd, in_background=False)

        elif test_type == "ncc":
            for sid in range(num_shards):
                server_name =tiga_common.TAG+"-server-s-"+str(sid).zfill(2)+"-r-"+str(0).zfill(2)
                log_file =  "server-log-s-"+str(sid)+"-r-"+str(0)                                
                server_cmd = (f"./deptran_server_ncc -b  "
                    f" -t {run_time_sec} "
                    f" -f ./config-tpl-n.yml -d 30 "
                    f" -P {server_name} > {log_file} 2>&1 &") 
                print_info(f"{server_name_dict[server_name]}\t{server_cmd}")
                run_command([server_name_dict[server_name]], 
                    server_cmd, in_background=False)

        elif test_type == "tiga":
            for sid in range(num_shards):
                for rid in range(num_replicas):
                    server_name = (tiga_common.TAG 
                        +"-server-s-"+str(sid).zfill(2)
                        +"-r-"+str(rid).zfill(2))
                    log_file = "server-log-s-"+str(sid)+"-r-"+str(rid) 
                    server_cmd = (f"GLOG_logtostderr=1 ./TigaServer "
                        " --config=./config-tpl-n.yml "
                        f" --serverName={server_name} "
                        f" --ioThreads=2 --workerNum=1 > {log_file} 2>&1 &")
                    print_info(f"{server_name_dict[server_name]}\t{server_cmd}")
                    run_command([server_name_dict[server_name]], 
                        server_cmd, in_background=False)
        elif test_type == "detock":
            for sid in range(num_shards):
                for rid in range(num_replicas):
                    server_name = (tiga_common.TAG 
                        +"-server-s-"+str(sid).zfill(2)
                        +"-r-"+str(rid).zfill(2))
                    log_file = "server-log-s-"+str(sid)+"-r-"+str(rid) 
                    server_cmd = (f"GLOG_logtostderr=1 ./DetockServer "
                        " --config=./config-tpl-n.yml "
                        f" --serverName={server_name} "
                        f" --ioThreads=2 --workerNum=1 > {log_file} 2>&1 &")
                    print_info(f"{server_name_dict[server_name]}\t{server_cmd}")
                    run_command([server_name_dict[server_name]], 
                        server_cmd, in_background=False)

        elif test_type == "calvin":
            for sid in range(num_shards):
                for rid in range(num_replicas):
                    server_name = (tiga_common.TAG 
                        +"-server-s-"+str(sid).zfill(2)
                        +"-r-"+str(rid).zfill(2))
                    log_file = "server-log-s-"+str(sid)+"-r-"+str(rid) 
                    server_cmd = (f"GLOG_logtostderr=1 ./CalvinServer "
                        " --config=./config-tpl-n.yml "
                        f" --serverName={server_name} "
                        f" --ioThreads=2 --workerNum=1 > {log_file} 2>&1 &")
                    print_info(f"{server_name_dict[server_name]}\t{server_cmd}")
                    run_command([server_name_dict[server_name]], 
                        server_cmd, in_background=False)

        else:
            for sid in range(num_shards):
                for rid in range(num_replicas):
                    server_name = (tiga_common.TAG 
                        +"-server-s-"+str(sid).zfill(2)
                        +"-r-"+str(rid).zfill(2))
                    log_file = "server-log-s-"+str(sid)+"-r-"+str(rid) 
                    server_cmd = (f"./deptran_server -b "
                        f" -t {run_time_sec} "
                        f" -f ./config-tpl-n.yml -d 30  "
                        f" -P {server_name} > {log_file} 2>&1 &")
                    print_info(f"{server_name_dict[server_name]}\t{server_cmd}")
                    run_command([server_name_dict[server_name]], 
                        server_cmd, in_background=False)

        time.sleep(10)
        # exit(0)


        # Launch proxies (id starts from 1)
        for i in range(num_proxies):
            process_name = proxy_names[i]
            log_file = "proxy-log-"+str(i)
            if tiga_common.is_remote_proxy(i):
                init_bound = tiga_common.TIGA_INIT_BOUND_ESTIMATION_REMOTE
            else:
                region_idx = tiga_common.get_region_idx(i)
                init_bound = tiga_common.TIGA_INIT_BOUND_ESTIMATION_LOCAL[region_idx]
            if test_type == 'tiga':
                proxy_cmd = (f"GLOG_logtostderr=1 ./TigaClient " 
                    f" --config=./config-tpl-n.yml "
                    f" --runTimeSec={run_time_sec} "
                    f" --serverName={process_name} "
                    f" --mcap={tiga_common.TIGA_CAP} " # maybe obsolete
                    f" --cap={tiga_common.TIGA_BOUND_CAP} "
                    f" --initBound={init_bound} "
                    f" --yieldPeriodUs={tiga_common.TIGA_YIELD_PERIOD_US} "
                    f" --logPrintUnit={tiga_common.LOG_UNIT} "
                    f" > {log_file} 2>&1 &")
            elif test_type == 'detock':
                proxy_cmd = (f"GLOG_logtostderr=1 ./DetockClient "
                    f" --config=./config-tpl-n.yml "
                    f" --runTimeSec={run_time_sec} "
                    f" --serverName={process_name} "
                    f" --mcap={tiga_common.DETOCK_CAP} "
                    f" --logPrintUnit={tiga_common.LOG_UNIT} "
                    f" > {log_file} 2>&1 &")
            elif test_type == 'calvin':
                designate_shard_id = 0
                designate_replica_id = 0
                if bench_type == 'Micro':
                    designate_shard_id = tiga_common.CALVIN_SEQUENCER_MAP_MICRO[i][0]
                    designate_replica_id = tiga_common.CALVIN_SEQUENCER_MAP_MICRO[i][1]
                elif bench_type == "TPCC":
                    designate_shard_id = tiga_common.CALVIN_SEQUENCER_MAP_TPCC[i][0]
                    designate_replica_id = tiga_common.CALVIN_SEQUENCER_MAP_TPCC[i][1]
                else:
                    print_error(f"bench_type not recognized {bench_type}")

                proxy_cmd = (f"GLOG_logtostderr=1 ./CalvinClient "
                    f" --config=./config-tpl-n.yml "
                    f" --runTimeSec={run_time_sec} "
                    f" --serverName={process_name} "
                    f" --designateShardId={designate_shard_id} "
                    f" --designateReplicaId={designate_replica_id} "
                    f" --mcap={tiga_common.CALVIN_CAP} "
                    f" --logPrintUnit={tiga_common.LOG_UNIT} "
                    f" > {log_file} 2>&1 &")
            elif test_type == 'ncc' or test_type == 'ncc-ft':
                proxy_cmd = (f"./deptran_server_ncc -b  "
                    f" -t {run_time_sec} "
                    f" -f ./config-tpl-n.yml -d 30  "
                    f" -P {process_name} > {log_file} 2>&1 &")

            else:
                proxy_cmd = (f"./deptran_server -b  "
                    f" -t {run_time_sec} "
                    f" -f ./config-tpl-n.yml -d 30  "
                    f" -P {process_name} > {log_file} 2>&1 &")

            print_info(proxy_ips[i]+"\t"+ proxy_cmd)
            run_command([proxy_ips[i]], proxy_cmd, in_background = True)
    

        time.sleep(run_time_sec + grace_period_sec)

        os.system(f"sudo rm -rf {tiga_common.STATS_PATH}/{case_name}")
        os.system(f"sudo mkdir -p -m777 {tiga_common.STATS_PATH}/{case_name}")

        for i in range(num_proxies):
            remote_path = f"{tiga_common.LOGIN_PATH}/{proxy_names[i]}.csv"
            local_path = f"{tiga_common.STATS_PATH}/{case_name}/Proxy-{i}.csv"
            scp_files([proxy_ips[i]], local_path, remote_path, to_remote=False)
            if i ==0:
                remote_path = f"{tiga_common.LOGIN_PATH}/config-tpl-n.yml" 
                local_path =  f"{tiga_common.STATS_PATH}/{case_name}/config-tpl-n.yml" 
                scp_files([proxy_ips[i]], local_path, remote_path, to_remote=False)

        end_time = time.time()
        duration = int(end_time-start_time)
        fpckpt.write(f"Done\t{case_name}-{end_time}--{duration} sec\n")
        fpckpt.flush()



        log_folder = tiga_common.LOG_PATH
        os.system("sudo rm -rf "+log_folder)
        os.system("sudo mkdir -m777 "+log_folder)

        if tiga_common.COLLECT_LOG is True:
            for sid in range(num_shards):
                for rid in range(num_replicas):
                    server_name = (tiga_common.TAG+"-server-s-"
                        +str(sid).zfill(2)+"-r-"+str(rid).zfill(2))
                    log_file = "server-log-s-"+str(sid)+"-r-"+str(rid) 
                    local_path = log_folder+"/"+log_file
                    remote_path = log_file
                    scp_files([server_name_dict[server_name]], local_path, remote_path, to_remote=False)
                    if test_type == 'ncc' or test_type == 'ncc-ft':
                        break
            for i in range(num_proxies):
                log_file = "proxy-log-"+str(i)
                local_path = log_folder+"/"+log_file
                remote_path = log_file
                scp_files(proxy_ips[i:(i+1)], local_path, remote_path, to_remote=False)


            if test_type == "ncc-ft":
                for sid in range(num_shards):
                    for rid in range(num_replicas):
                        server_name = (tiga_common.TAG+"-server-s-"
                            +str(sid).zfill(2)+"-r-"+str(rid).zfill(2))
                        log_file = "replica-log-s-"+str(sid)+"-r-"+str(rid) 
                        local_path = log_folder+"/"+log_file
                        remote_path = log_file
                        scp_files([server_name_dict[server_name]], 
                            local_path, remote_path, to_remote=False)

    if tiga_common.SHUTDOWN_AFTER_RUN is True:
        print_info("To stop instance in 10 sec")
        time.sleep(10)
        print_info("Stoping...")
        for region in tiga_common.SERVER_REGIONS:
            info_arr = gcp_tools.get_instance_info_by_tag(
                tiga_common.TAG +"-server-s-", region)
            instances_to_start = []
            for info in info_arr:
                instance_name = info[0]
                if instance_name in server_names:
                    instances_to_start.append(instance_name)
            gcp_tools.stop_instance_list(instances_to_start, region)

        for region in tiga_common.ALL_REGIONS:
            info_arr = gcp_tools.get_instance_info_by_tag(
                tiga_common.TAG+"-proxy-", region)
            instances_to_start = []
            for info in info_arr:
                instance_name = info[0]
                if instance_name in proxy_names:
                    instances_to_start.append(instance_name)
            gcp_tools.stop_instance_list(instances_to_start, region)