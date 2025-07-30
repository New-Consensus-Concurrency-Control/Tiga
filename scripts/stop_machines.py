import tiga_common
import gcp_tools


if __name__ == '__main__':
    parser = tiga_common.argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--num_replicas',  type=int, default = 3,
                        help='Specify the number of replicas ')
    parser.add_argument('--num_shards',  type=int, default = 3,
                        help='Specify the number of shards ')
    parser.add_argument('--num_proxies',  type=int, default = 8,
                        help='Specify the number of proxies ')

    args = parser.parse_args()
    num_replicas = args.num_replicas
    num_shards = args.num_shards
    num_servers = num_replicas * num_shards
    num_proxies = args.num_proxies 
    tiga_common.print_info(f"replicas: {num_replicas}")
    tiga_common.print_info(f"shards: {num_shards}")
    tiga_common.print_info(f"proxies: {num_proxies}")

    server_names, server_ips = gcp_tools.get_ips_by_name(
        cluster_name= tiga_common.TAG+"-server-", 
        zone=",".join(tiga_common.SERVER_REGIONS))
    proxy_names, proxy_ips = gcp_tools.get_ips_by_name(
        cluster_name=tiga_common.TAG+"-proxy-", 
        zone=",".join(tiga_common.ALL_REGIONS))

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