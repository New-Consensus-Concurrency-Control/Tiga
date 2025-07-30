import tiga_common
import gcp_tools
import cwcs_tools


if __name__ == '__main__':
    parser = tiga_common.argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--num_replicas',  type=int, default = 3,
                        help='Specify the number of replicas ')
    parser.add_argument('--num_shards',  type=int, default = 3,
                        help='Specify the number of shards ')
    parser.add_argument('--num_proxies',  type=int, default = 8,
                        help='Specify the number of proxies ')
    parser.add_argument('--clock_sync',  type=str, default = "chrony",
                        help='Specify the type of clock sync (chrony|cwcs|ntp)')
    parser.add_argument('--action',  type=str, default = "stop",
                        help='Specify the action (install|start|stop)')

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
        zone= ",".join(tiga_common.ALL_REGIONS))
    
    all_ips = server_ips[0:num_servers] + proxy_ips[0:num_proxies]
    if args.clock_sync == 'cwcs':
        if args.action == 'install':
            cwcs_tools.start_ttcs_batches(
                all_ips, False, False) 
        elif args.action == 'start':
            cwcs_tools.launch_ttcs(all_ips) 
        elif args.action == 'stop':
            cwcs_tools.stop_ttcs(all_ips)  
        else:
            tiga_common.print_error(
                f"Unrecognized action {args.action}")
    elif args.clock_sync == 'chrony':
        if args.action == 'install':
            tiga_common.print_info(
                "Chrony should have been installed by default")
        elif args.action == 'start':
            tiga_common.run_command(all_ips, 
                "sudo service chrony start", 
                in_background=False)
        elif args.action == 'stop':
            tiga_common.run_command(all_ips, 
                "sudo service chrony stop", 
                in_background=False)
        else:
            tiga_common.print_error(
                f"Unrecognized action {args.action}") 
    elif args.clock_sync == 'ntp':
        if args.action == 'install':
            tiga_common.print_info(
                "Ntp should have been installed by default")
        elif args.action == 'start':
            tiga_common.run_command(all_ips, 
                "sudo systemctl start ntp", 
                in_background=False)
        elif args.action == 'stop':
            tiga_common.run_command(all_ips, 
                "sudo systemctl stop ntp", 
                in_background=False)
        else:
            tiga_common.print_error(
                f"Unrecognized action {args.action}")  
    else:
        tiga_common.print_error(
            f"Unrecognized clock_sync {args.clock_sync}")
