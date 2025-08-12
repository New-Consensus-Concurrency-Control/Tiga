import pandas as pd
from IPython import embed; 
import argparse
import datetime
import os
import tiga_common

def throughput_apply_func(group):
    if len(group):
        return pd.Series({
            'SendTime':  list(group['SendTime'])[0],
            'AvgThroughput':len(group),
            'MeanLatency': round(group["ProxyLatency"].mean(),2)
        })



def ThroughputAnalysis(merge_df):
    merge_df.loc[:, "time"] = merge_df['CommitTime'].apply(
                lambda us_ts: datetime.datetime.fromtimestamp(us_ts * 1e-6))
    bin_interval_ms = 1000
    grouped = merge_df.groupby(
        pd.Grouper(key='time', freq='{}ms'.format(bin_interval_ms)))
    grouped_apply_orders = grouped.apply(throughput_apply_func, include_groups=False)
    grouped_apply_orders = grouped_apply_orders.dropna()
    grouped_apply_orders = grouped_apply_orders.reset_index()
    grouped_apply_orders = grouped_apply_orders[5:-5]
    grouped_apply_orders['AvgThroughput'] =    grouped_apply_orders['AvgThroughput']*1000/bin_interval_ms
    print("AvgThroughput2: ", list(grouped_apply_orders['AvgThroughput']))

    duration = len(grouped_apply_orders)
    throughput = (grouped_apply_orders['AvgThroughput']).mean()
    return throughput, duration


def OutputStats(target_proxies, showPure=False):
    df_list = []
    for i in target_proxies:
        file_name = "Proxy-"+str(i)+".csv"
        fname = stats_folder+"/"+file_name
        if os.path.exists(fname):
            proxy_df = pd.read_csv(fname)
            proxy_df['ProxyID']=i
            df_list.append(proxy_df)
        else:
            print(fname, " Not exists")
    all_df = pd.concat(df_list)
    all_df['ProxyLatency'] = all_df['CommitTime']-all_df['SendTime']
    all_df['ProxyLatency'] =  all_df['ProxyLatency']/1000
    # all_df = all_df[all_df["ClientID"] % 2 ==0]


    stats = ""
    stats += "Num:"+str(len(all_df))+"\n"
    stats += "P-25p:\t"+str(int(all_df['ProxyLatency'].quantile(.25)))+"\t\t"
    stats += "P-50p:\t"+str(int(all_df['ProxyLatency'].quantile(.5)))+"\n"
    stats += "P-75p:\t"+str(int(all_df['ProxyLatency'].quantile(.75)))+"\t\t"
    stats += "P-90p:\t"+str(int(all_df['ProxyLatency'].quantile(.9)))+"\n"
    stats += "P-95p:\t"+str(int(all_df['ProxyLatency'].quantile(.95)))+"\t\t"
    stats += "P-99p:\t"+str(int(all_df['ProxyLatency'].quantile(.99)))+"\n"

    if(showPure) and 'preDisPatchCompleteTime' in all_df.columns :
        all_df["PureLatency"] = all_df["CommitTime"] - all_df['preDisPatchCompleteTime']
        all_df['PureLatency'] =  all_df['PureLatency']/1000
        stats += "Pure"+"\n"
        stats += "R-25p:\t"+str(all_df['PureLatency'].quantile(.25))+"\n"
        stats += "R-50p:\t"+str(all_df['PureLatency'].quantile(.5))+"\n"
        stats += "R-75p:\t"+str(all_df['PureLatency'].quantile(.75))+"\n"
        stats += "R-90p:\t"+str(all_df['PureLatency'].quantile(.9))+"\n"
        stats += "R-95p:\t"+str(all_df['PureLatency'].quantile(.95))+"\n"
        stats += "R-99p:\t"+str(all_df['PureLatency'].quantile(.99))+"\n"

    if 'Bound' in all_df.columns:
        stats += "Avg Bound:\t"+str(int(all_df['Bound'].mean()/1000))+"\n"

    # if 'RepSlow' in all_df.columns:
    #     slow_df =  all_df[all_df['RepSlow']>0]
    #     stats += "Slow Rate:\t"+str(len(slow_df)*100/len(all_df) ) +"\n"

    if 'NonSerial' in all_df.columns:
        non_serial_df = all_df[all_df['NonSerial']>0]
        stats += "Rollback Rate:\t"+str(len(non_serial_df)*100/len(all_df) ) +"\n"

    if 'nTry' in all_df.columns:
        retried_df = all_df[all_df['nTry']>1]
        aborted_txn_num = retried_df['nTry'].sum()
        print("retried_df=",len(retried_df))
        print("aborted_txn_num=", aborted_txn_num)
        print("all_df=", len(all_df))
        stats += "Abort Rate:\t"+str(aborted_txn_num/ (len(all_df)+ aborted_txn_num) )+"\n"
        stats += "Aborted Txn Ratio:\t"+str(len(retried_df)/ len(all_df) )+"\n"

    print(stats)

    throughput_stats, duration = ThroughputAnalysis(all_df)
    print("Throughput ", throughput_stats, "\t", duration, "\tseconds")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--num_proxies',  type=int, default = 8,
                        help='Specify the number of proxies ')
    parser.add_argument('--num_local_proxies',  type=int, default = 6,
                        help='Specify the number of co-located proxies ')
    parser.add_argument('--prefix',  type=str, default = "",
                        help='')
    parser.add_argument('--stats_path',  type=str, default = "",
                        help='')
    args = parser.parse_args()

    num_proxies = args.num_proxies

    print("proxies: ", num_proxies)

    if args.prefix == "" :
        stats_folder = "/mnt/disks/data/"+args.stats_path
    else:
        stats_folder =args.prefix +"/" + args.stats_path
    print("Stats folder ", stats_folder)
    

    if "janus" in stats_folder:
        show_pure = True 
    else:
        show_pure = False

    tiga_common.print_info("Region-0:")
    OutputStats([0,3])
    tiga_common.print_info("Region-1:")
    OutputStats([1,4])
    tiga_common.print_info("Region-2:")
    OutputStats([2,5])
    if args.num_local_proxies < args.num_proxies:
        tiga_common.print_info("Region-4 (Remote): ")
        OutputStats(range(args.num_local_proxies, args.num_proxies), show_pure)

    tiga_common.print_info("All Regions: ")
    OutputStats(range(8))

