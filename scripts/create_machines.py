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
    parser.add_argument('--num_local_proxies',  type=int, default = 6,
                        help='Specify the number of local proxies ')
    args = parser.parse_args()

    num_replicas = args.num_replicas
    num_shards = args.num_shards
    num_servers = num_replicas * num_shards
    num_proxies = args.num_proxies 
    print("replicas: ", num_replicas)
    print("shards: ", num_shards)
    print("proxies: ", num_proxies)

    server_name_list = []
    for s in range(num_shards):
        for r in range(num_replicas):
            server_name = (tiga_common.TAG+"-server-s-"
                +str(s).zfill(2)+"-r-"+str(r).zfill(2))
            server_name_list.append(server_name)
    proxy_name_list = [ tiga_common.TAG+"-proxy-"+str(i).zfill(4) 
                        for i in range(num_proxies) ]



    for s in range(num_shards):
        for r in range(num_replicas):
            server_name = tiga_common.TAG+"-server-s-"+str(s).zfill(2)+"-r-"+str(r).zfill(2)
            gcp_tools.create_instance(instance_name = server_name,
                            image= tiga_common.IMAGE_FAMILY,
                            machine_type =  tiga_common.SERVER_VM_TYPE,
                            customzedZone=tiga_common.SERVER_REGIONS[r])
            tiga_common.print_good(f"Created {server_name}")
    
    for i in range(num_proxies):
        if i < 2 * len(tiga_common.SERVER_REGIONS): # local region
            region = tiga_common.SERVER_REGIONS[i % len(tiga_common.SERVER_REGIONS)]
        else:
            region = tiga_common.REMOTE_REGIONS[0]
        gcp_tools.create_instance(instance_name = proxy_name_list[i],
                        image= tiga_common.IMAGE_FAMILY,
                        machine_type =  tiga_common.PROXY_VM_TYPE,
                        customzedZone=region)
        tiga_common.print_good(f"Created {proxy_name_list[i]}")
        
    exit(0)
