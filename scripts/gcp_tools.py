import os
import subprocess
from subprocess import PIPE, Popen
import tiga_common

def create_instance(instance_name,
                    image=None,
                    machine_type = "n1-standard-4",
                    customzedZone = "us-central1-a",
                    customzedIp = None,
                    require_external_ip=False,
                    second_ip = False
                    ):
    # Construct gcloud command to create instance.
    network_address_config = ("--network-interface no-address"
                              if require_external_ip == False else "")
    
    if customzedIp is not None:
        network_address_config += ",private-network-ip="+customzedIp
        
    if second_ip:
        network_address_config += " --network-interface subnet=subnet-1,no-address"
    # scopes = "--scopes storage-full,https://www.googleapis.com/auth/bigtable.admin,https://www.googleapis.com/auth/bigtable.data,https://www.googleapis.com/auth/bigquery"
    # if full_access_to_cloud_apis:
    scopes = "--scopes=https://www.googleapis.com/auth/cloud-platform"

    create_instance_cmd = """gcloud beta compute instances create {inst} --zone {zone} --image-family {source_image} --machine-type {machine_type} {network} {scopes} --boot-disk-size 50GB""".format(
        inst=instance_name,
        zone=customzedZone,
        source_image=image,
        machine_type=machine_type,
        network=network_address_config,
        scopes=scopes,
    )

    # print(create_instance_cmd)
    # Run gcloud command to create machine.
    proc = Popen(create_instance_cmd, stdout=PIPE, stderr=PIPE, shell=True)
    # Wait for the process end and print error in case of failure
    output, error = proc.communicate()
    if proc.returncode != 0:
        tiga_common.print_error(f"Failed to create instance")
        tiga_common.print_error(f"Error Response:  {output} {error}")



def del_instance_list(instance_list, zone="us-central1-a"):
    for machine in instance_list:
        tiga_common.print_info(f"Deleting {machine}")
        subprocess.Popen(
            'gcloud -q compute instances delete {inst} --zone {zone}'.format(
                inst=machine, zone=zone).split())




def start_instance_list(instance_list, zone="us-central1-a"):
    if(len(instance_list)==0):
        return
    start_cmd = 'gcloud compute instances start {inst} --zone {zone}'.format(
            inst=' '.join(instance_list), zone = zone
            )
    tiga_common.print_info(start_cmd)
    os.system(start_cmd)


def modify_instance_list(instance_list, machine_type="n1-standard-8",  zone="us-central1-a"):
    for inst in instance_list:
        modify_cmd = 'gcloud compute instances set-machine-type  {inst} --machine-type {mt} --zone {zone}'.format(
                inst=inst, zone = zone, mt=machine_type
                )
        tiga_common.print_info(modify_cmd)
        os.system(modify_cmd)
        
def stop_instance_list(instance_list, zone="us-central1-a"):
    stop_cmd = 'gcloud compute instances stop {inst} --zone {zone}'.format(
            inst=' '.join(instance_list), zone = zone
            )
    print(stop_cmd)
    os.system(stop_cmd)


def get_ips_by_name(cluster_name, zone="us-central1-a"):
    cmd = 'gcloud compute instances list --zones {zone}'.format(zone=zone)
    tiga_common.print_info(cmd)
    gcloud_output = subprocess.check_output(cmd.split())
    data = gcloud_output.decode("utf-8").split('\n')
    names = []
    iips = []
    for line in data:
        if cluster_name in line:
            tokens = line.strip().split()
            iip = tokens[3]
            iips.append(iip)
            name = tokens[0]
            names.append(name)
    if len(names) >0:
        names, iips = zip(*sorted(zip(names, iips)))
        return list(names), list(iips)
    else:
        return [], []



def get_instance_info_by_tag(tag, zone="us-central1-a"):
    cmd = 'gcloud compute instances list --zones {zone}'.format(zone=zone)
    gcloud_output = subprocess.check_output(cmd.split())
    data = gcloud_output.decode("utf-8").split('\n')
    info_arr = []
    for line in data:
        if tag in line:
            info = line.strip().split()
            info_arr.append(info)
    return info_arr