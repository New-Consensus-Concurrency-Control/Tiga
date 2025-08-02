import tiga_common
import gcp_tools
import ruamel.yaml

run_command = tiga_common.run_command
scp_files = tiga_common.scp_files



def generate_ttcs_cfg_file(internal_ip, is_reference=False, use_ntp=False):
    yaml = ruamel.yaml.YAML()
    cwcs_f = open("cwcs-agent-tpl.yaml", "r")
    cwcs = yaml.load(cwcs_f)
    cwcs["management_address"] = internal_ip
    cwcs["subscription_mode"] = True 
    cwcs["subscription_endpoints"] = [tiga_common.TTCS_COORDINATOR_IP+":6176"]
    cwcs["probe_address"] = internal_ip
    cwcs["clock_quality"] = 1    
    if is_reference:
        cwcs["clock_quality"] = 10
        cwcs["correct_clock"] = True 

    if use_ntp:
        cwcs["correct_clock"] = False 
    else:
        cwcs["correct_clock"] = True 
    flag = cwcs["correct_clock"]
    tiga_common.print_info(f"cwcs[correct_clock]={flag}")
    out_f = open("cwcs-agent.yaml", "w")
    yaml.dump(cwcs, out_f);


def start_ttcs_batches(internal_ip_list, is_reference, use_ntp=False):
    clean_prev_deb_cmd = "sudo dpkg -P cwcs-agent"
    run_command(internal_ip_list, clean_prev_deb_cmd, in_background=False)
    ttcs_binary = f"{tiga_common.LOGIN_PATH}/cwcs-agent_2.0.0_amd64.deb"
    scp_files(internal_ip_list, ttcs_binary, ttcs_binary, to_remote = True)
    install_deb_cmd = f"sudo dpkg -i {tiga_common.LOGIN_PATH}/cwcs-agent_2.0.0_amd64.deb"
    run_command(internal_ip_list, install_deb_cmd, in_background=False)
    
    remote_dir = "/etc/opt/cwcs"
    remote_path = remote_dir + "/cwcs-agent.yaml"
    chmod_cmd = f"sudo chmod -R 777 {remote_dir}"
    run_command(internal_ip_list, chmod_cmd, in_background=False)

    rm_cmd = f"sudo rm -f {remote_path}"
    run_command(internal_ip_list, rm_cmd, in_background=False)

    for internal_ip in internal_ip_list:
        cfg_file = generate_ttcs_cfg_file(internal_ip, is_reference, use_ntp)
        local_file_path = "./cwcs-agent.yaml"
        scp_files([internal_ip], local_file_path, remote_path, to_remote=True)


    stop_ntp_cmd = "sudo service ntp stop"
    run_command(internal_ip_list, stop_ntp_cmd, in_background=False)
    disable_ntp_cmd = "sudo systemctl disable ntpd"
    run_command(internal_ip_list, disable_ntp_cmd, in_background=False)

    stop_ntp_cmd = "sudo service chrony stop"
    run_command(internal_ip_list, stop_ntp_cmd, in_background=False)
    disable_ntp_cmd = "sudo systemctl disable chronyd"
    run_command(internal_ip_list, disable_ntp_cmd, in_background=False)

    sys_start_ttcp_agent_cmd = "sudo systemctl start cwcs-agent"
    run_command(internal_ip_list, sys_start_ttcp_agent_cmd, in_background=False)

def observe_ttcs(internal_ip_list):
    remote_dir = "/etc/opt/cwcs"
    remote_path = remote_dir + "/cwcs-agent.yaml"
    chmod_cmd = f"sudo chmod -R 777 {remote_dir}"
    run_command(internal_ip_list, chmod_cmd, in_background=False)
    rm_cmd = f"sudo rm -f {remote_path}"
    run_command(internal_ip_list, rm_cmd, in_background=False)
    for internal_ip in internal_ip_list:
        cfg_file = generate_ttcs_cfg_file(internal_ip, is_reference=False, use_ntp = True)
        local_file_path = "./cwcs-agent.yaml"
        scp_files([internal_ip], local_file_path, remote_path, to_remote=True)
    sys_start_ttcp_agent_cmd = "sudo systemctl start cwcs-agent"
    run_command(internal_ip_list, sys_start_ttcp_agent_cmd, in_background=False)



def resync_ttcs(internal_ip_list):
    remote_dir = "/etc/opt/cwcs"
    remote_path = remote_dir + "/cwcs-agent.yaml"
    chmod_cmd = f"sudo chmod -R 777 {remote_dir}"
    run_command(internal_ip_list, chmod_cmd, in_background=False)
    rm_cmd = f"sudo rm -f {remote_path}"
    run_command(internal_ip_list, rm_cmd, in_background=False)
    for internal_ip in internal_ip_list:
        cfg_file = generate_ttcs_cfg_file(internal_ip, is_reference=False, use_ntp = False)
        local_file_path = "./cwcs-agent.yaml"
        scp_files([internal_ip], local_file_path, remote_path, to_remote=True)
    sys_start_ttcp_agent_cmd = "sudo systemctl start cwcs-agent"
    run_command(internal_ip_list, sys_start_ttcp_agent_cmd, in_background=False)



def launch_ttcs(server_ip_list, use_ntp=False):
    stop_ntp_cmd = "sudo service chrony stop"
    run_command(server_ip_list, stop_ntp_cmd, in_background=False)
    disable_ntp_cmd = "sudo systemctl disable chronyd"
    run_command(server_ip_list, disable_ntp_cmd, in_background=False)
    if use_ntp:
        start_ntp_cmd = "sudo systemctl start ntp"
        run_command(server_ip_list, start_ntp_cmd, in_background=False)
        enable_ntp_cmd = "sudo systemctl enable ntp"
        run_command(server_ip_list, enable_ntp_cmd, in_background=False)
    else:
        stop_ntp_cmd = "sudo systemctl stop ntp"
        run_command(server_ip_list, stop_ntp_cmd, in_background=False)
        disable_ntp_cmd = "sudo systemctl disable ntp"
        run_command(server_ip_list, disable_ntp_cmd, in_background=False)
    sys_start_ttcp_agent_cmd = "sudo systemctl start cwcs-agent"
    run_command(server_ip_list, sys_start_ttcp_agent_cmd, in_background=False)

def stop_ttcs(server_ip_list):
    stop_cmd = "sudo systemctl stop cwcs-agent"
    run_command(server_ip_list, stop_cmd, in_background=False)
