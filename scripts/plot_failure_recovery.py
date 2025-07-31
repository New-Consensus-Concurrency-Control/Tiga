import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from IPython import embed; 
import argparse
import datetime
import os
import tiga_common
import ruamel.yaml
from pandas.api.types import CategoricalDtype

get_key_value = tiga_common.get_key_value

plt.rcParams.update({'font.size': 12, "font.weight":"normal"})
fig_size = (3.3,2.3)

def throughput_apply_func(group):
    if len(group):
        return pd.Series({
            'SendTime':  list(group['SendTime'])[0],
            'AvgThroughput':len(group),
            'MeanLatency': round(group["ProxyLatency"].mean(),2)
        })

def ThroughputAnalysis2(merge_df):
    merge_df.loc[:, "time"] = merge_df['CommitTime'].apply(
                lambda us_ts: datetime.datetime.fromtimestamp(us_ts * 1e-6))
    bin_interval_ms = 100
    grouped = merge_df.groupby(
        pd.Grouper(key='time', freq='{}ms'.format(bin_interval_ms)))
    grouped_apply_orders = grouped.apply(throughput_apply_func)

    grouped_apply_orders = grouped_apply_orders.dropna()
    grouped_apply_orders = grouped_apply_orders.reset_index()
    grouped_apply_orders['AvgThroughput'] =  grouped_apply_orders['AvgThroughput']*1000/bin_interval_ms
    duration = len(grouped_apply_orders)
    tp_list = list(grouped_apply_orders['AvgThroughput'])
    latency_list = list(grouped_apply_orders['MeanLatency'])
    return tp_list, latency_list, duration

    

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

    yaml = ruamel.yaml.YAML()
    test_plan_f = open(test_plan_file, 'r')
    test_plan = yaml.load(test_plan_f)
    case = test_plan[0]
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
    for i in range(num_proxies):
        fname = f"{csv_path}/Proxy-{i}.csv"
        if os.path.exists(fname):
            proxy_df = pd.read_csv(fname)
            proxy_df['ProxyID']=i 
            proxy_df["RegionID"]= tiga_common.PROXY_REGIONS[i]
            df_list.append(proxy_df)
        else:
            tiga_common.print_error(f"{fname} Not exists")
    all_df = pd.concat(df_list)
    all_df['ProxyLatency'] = all_df['CommitTime']-all_df['SendTime']
    all_df['ProxyLatency'] =  all_df['ProxyLatency']/1000
    tp_list, latency_list, duration =  ThroughputAnalysis2(all_df)

    xmin = 0
    xmax = 15
    ymin = 0
    ymax = 200
    x_label = "Time (s)"
    y_label = "Throughput ($10^3$ txns/s)"

    new_tp_list = [x *0.001 for x in tp_list]
    new_tp_list = new_tp_list[50:-50]  # 5s-15s
    xx_list = range(0, len(new_tp_list))
    x_list = [x*0.1 for x in xx_list]
    fig = plt.figure(figsize=fig_size)
    # plt.scatter(x_list, new_tp_list)
    plt.plot(x_list, new_tp_list)
    plt.xlim([xmin, xmax])
    plt.ylim([ymin, ymax])
    # plt.xticks(x_ticks)
    plt.yticks([0,100,200])
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.savefig(
        f'{tiga_common.FIGS_PATH}/failure-recovery-thpt.pdf',
        bbox_inches='tight', dpi=1200)
    tiga_common.print_info(
        f"saved {tiga_common.FIGS_PATH}/failure-recovery-thpt.pdf")


    region_df = all_df[
        all_df["RegionID"]==len(tiga_common.REGION_PROXIES)-1
    ]
    region_df = region_df.copy()
    tp_list, latency_list, duration =  ThroughputAnalysis2(region_df)

    xmin = 0
    xmax = 15
    ymin = 0
    ymax = 600
    x_label = "Time (s)"
    y_label = "50p Latency (ms)"

    new_latency_list = latency_list[50:-50]
    x_list = range(0, len(new_latency_list))
    x_list = [x*0.1 for x in x_list]
    fig = plt.figure(figsize=fig_size)
    # plt.scatter(x_list, new_tp_list)
    plt.plot(x_list, new_latency_list)
    plt.xlim([xmin, xmax])
    plt.ylim([ymin, ymax])
    # plt.xticks(x_ticks)
    # plt.yticks(y_ticks)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.savefig(
        f'{tiga_common.FIGS_PATH}/failure-recovery-latency-region.pdf',
        bbox_inches='tight', dpi=1200)
    tiga_common.print_info(
        f"saved {tiga_common.FIGS_PATH}/failure-recovery-latency-region.pdf")