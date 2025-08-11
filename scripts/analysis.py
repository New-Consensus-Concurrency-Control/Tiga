import pandas as pd
import numpy as np
from IPython import embed; 
import argparse
import datetime
import os
import tiga_common
import ruamel.yaml


print_error = tiga_common.print_error
print_info = tiga_common.print_info 
print_good = tiga_common.print_good 
get_key_value = tiga_common.get_key_value

def throughput_apply_func(group):
    if len(group):
        return pd.Series({
            'AvgThroughput':len(group),
        })
def ThroughputAnalysis(merge_df):
    merge_df.loc[:, "time"] = merge_df['CommitTime'].apply(
                lambda us_ts: datetime.datetime.fromtimestamp(us_ts * 1e-6))
    bin_interval_s = 1
    grouped = merge_df.groupby(
        pd.Grouper(key='time', freq='{}s'.format(bin_interval_s)))
    grouped_apply_orders = grouped.apply(throughput_apply_func, include_groups=False)
    grouped_apply_orders = grouped_apply_orders.dropna()
    # grouped_apply_orders = grouped_apply_orders[5:-5]
    # print(list(grouped_apply_orders['AvgThroughput']))
    duration = len(grouped_apply_orders)
    throughput = (grouped_apply_orders['AvgThroughput']/bin_interval_s).mean()
    return throughput, duration

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


    yaml = ruamel.yaml.YAML()
    test_plan_f = open(test_plan_file, 'r')
    test_plan = yaml.load(test_plan_f)


    header = "BenchType\tTestType\tTestNo\tRate\tSkew\t50p\t90p\tThroughput\tCommitRate\tRegionIdx\tPrefix"
    print(header)
     
    stats_csv_files = [
        test_plan_file.split(".")[0]+f"-region-{r}.csv" 
        for r in range(len(tiga_common.REGION_PROXIES))
    ] + [
        test_plan_file.split(".")[0]+f"-region-all.csv" 
    ]
    os.system(f"sudo mkdir -m777 -p {tiga_common.SUMMARY_STATS_PATH}")
    stats_fs = [ 
        open(f"{tiga_common.SUMMARY_STATS_PATH}/{stats_csv_file}", "w")
        for stats_csv_file in stats_csv_files
    ]
    for region_idx in  range(len(stats_csv_files)):
        print_info(f"stats file {stats_csv_files[region_idx]}")
        stats_fs[region_idx].write(header+"\n")


    for case in test_plan:
        prefix = get_key_value(case, "prefix", "", False)
        test_no = get_key_value(case, "test_no", 0, False)
        bench_type = get_key_value(case, "bench_type", "Micro", False)
        rate = get_key_value(case, "rate", 1000, False)
        preventive = get_key_value(case, "preventive", True, False)
        test_type = get_key_value(case, "test_type", "tiga", False)
        leader_rotation = get_key_value(case, "leader_rotation", False, False)
        skew_factor = get_key_value(case, "skew_factor", 0.5, False)
        key_num = get_key_value(case, "key_num", 1000000, False)
        owd_delta_us = get_key_value(case, "owd_delta_us", 10000, False)
        owd_estimate_percentile = get_key_value(
            case, "owd_estimate_percentile", 50, False)
        test_failure_recovery = get_key_value(
            case, "test_failure_recovery", False, False)
        synced_logid_before_failure = get_key_value(
            case, "synced_logid_before_failure", 600000, False)
        clock_approach = get_key_value(
            case, "clock_approach", "cwcs", False)

        # Generate configs
        if bench_type == "TPCC":
            config_template = f"{tiga_common.CONFIG_PATH}/config-local-tpcc.yml"
            state_machine_info = ""
            skew_factor = 0
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
        # print_info(case_name)
        csv_path = f"{tiga_common.STATS_PATH}/{case_name}"
        df_list = []
        df_complete = True
        for i in range(num_proxies):
            fname = f"{csv_path}/Proxy-{i}.csv"
            if os.path.exists(fname):
                proxy_df = pd.read_csv(fname)
                proxy_df['ProxyID']=i 
                proxy_df["RegionID"]= tiga_common.PROXY_REGIONS[i]
                df_list.append(proxy_df)
            else:
                tiga_common.print_error(f"{fname} Not exists")
                df_complete = False
        if df_complete is False:
            print_info(f"This case might not run successfully: {case_name}")
            continue
        all_df = pd.concat(df_list)
        if(len(all_df)<100):
            print_info(f"This case might not run successfully: {case_name}")
            continue
        all_df['ProxyLatency'] = all_df['CommitTime']-all_df['SendTime']
        all_df['ProxyLatency'] =  all_df['ProxyLatency']/1000
        if (test_type == 'janus' 
            and bench_type=='Micro'
            and "preDisPatchCompleteTime" in all_df.columns): # Janus adds one unnecessary hop for Micro
            all_df["ProxyLatency"] = all_df["CommitTime"] - all_df['preDisPatchCompleteTime']
            all_df['ProxyLatency'] =  all_df['ProxyLatency']/1000
        
        for region_idx in range(len(stats_fs)):
            if region_idx < len(tiga_common.REGION_PROXIES):
                region_df = all_df[all_df["RegionID"]==region_idx]
            else:
                region_df = all_df 

            if 'nTry' in region_df.columns:
                retried_df = region_df[region_df['nTry']>1]
                aborted_txn_num = retried_df['nTry'].sum()
                # print(f"aborted_txn_num={aborted_txn_num} len(region_df)={len(region_df)}")
                abort_rate = aborted_txn_num/ (len(region_df)+ aborted_txn_num) 
            else: 
                abort_rate = 0
            stats = f"{bench_type}\t{test_type}\t{test_no}\t{rate}\t{skew_factor}\t"
            stats += str(int(region_df['ProxyLatency'].quantile(.5)))+"\t"
            stats += str(int(region_df['ProxyLatency'].quantile(.9)))+"\t"
            throughput_stats, duration = ThroughputAnalysis(region_df.copy())
            stats += str(np.round(throughput_stats*0.001,decimals=2))+"\t"
            stats += str(int(100-abort_rate*100))+"\t"
            stats += str(int(region_idx))+"\t"
            if prefix == "":
                stats += "NULL"
            else:
                stats += prefix
            print(stats)
            stats_fs[region_idx].write(stats +"\n")
    for stats_f in stats_fs:
        stats_f.close()
    