
remote_port = 3306

pc_info = {'local_hostname': 'localhost',
           'local_port': 3306,

            # vps1 ------------------------
            'vps1_remote_username': 'tsetmc_raw_data_',
            'vps1_remote_password': '9!b$#4Kx7W5g2L#pvmEta7Afc#6PgYz7mmaexr@2@!zR4hgt/NM!nb$zAP-Y&eULz6KqW%^EUZAd6XsywpqfYgzS^RXmnv_v@4JF76xeU=z3KSe7',
            'vps1_remote_hostname': '178.63.149.85',
            'vps1_remote_port': remote_port,
            # ----------------------
            'vps1_local_username': 'tsetmc_raw_data',
            'vps1_local_password': '9!b$#4Kx7W5g2L#p',

            # server ------------------------
            'server_remote_username_vps': 'bours_R_vps',
            'server_remote_password_vps': '!mVVx2*Mr3WRNyVh19Uxc%pAk',
            'server_remote_hostname': '2.184.236.202',
            'server_remote_port': remote_port,
            # ----------------------
            'server_local_username': 'bourse_user',
            'server_local_password': 'Asdf1234',

            # ----------------------
            'server_lan_username': 'bourse_user_g',
            'server_lan_password': 'Asdf1234',
            'server_lan_hostname': '192.168.1.5',
            'server_lan_port': remote_port,

            # server ------------------------
            'laptop_local_username': 'bourse_user',
            'laptop_local_password': 'Asdf1234',
           }

vps1_local_access = 'vps1_local_access'
vps1_remote_access = 'vps1_remote_access'
server_local_access = 'server_local_access'
server_remote_access_from_vps = 'server_remote_access_from_vps'
server_lan_access = 'server_lan_access'
laptop_local_access = 'laptop_local_access'


def get_database_info(pc_name, database_name):
    res = None
    # local ----------------------
    if pc_name == 'vps1_local_access':
        res = {'db_name': database_name,
               'db_username': pc_info['vps1_local_username'],
               'db_user_password': pc_info['vps1_local_password'],
               'db_host_name': pc_info['local_hostname'],
               'db_port': pc_info['local_port']}

    elif pc_name == 'server_local_access':
        res = {'db_name': database_name,
               'db_username': pc_info['server_local_username'],
               'db_user_password': pc_info['server_local_password'],
               'db_host_name': pc_info['local_hostname'],
               'db_port': pc_info['local_port']}

    elif pc_name == 'laptop_local_access':
        res = {'db_name': database_name,
               'db_username': pc_info['laptop_local_username'],
               'db_user_password': pc_info['laptop_local_password'],
               'db_host_name': pc_info['local_hostname'],
               'db_port': pc_info['local_port']}
    # remote ----------------------
    elif pc_name == 'vps1_remote_access':
        res = {'db_name': database_name,
               'db_username': pc_info['vps1_remote_username'],
               'db_user_password': pc_info['vps1_remote_password'],
               'db_host_name': pc_info['vps1_remote_hostname'],
               'db_port': pc_info['vps1_remote_port']}

    elif pc_name == 'server_remote_access_from_vps':
        res = {'db_name': database_name,
               'db_username': pc_info['server_remote_username_vps'],
               'db_user_password': pc_info['server_remote_password_vps'],
               'db_host_name': pc_info['server_remote_hostname'],
               'db_port': pc_info['server_remote_port']}

    # lan ----------------------
    elif pc_name == 'server_lan_access':
        res = {'db_name': database_name,
               'db_username': pc_info['server_lan_username'],
               'db_user_password': pc_info['server_lan_password'],
               'db_host_name': pc_info['server_lan_hostname'],
               'db_port': pc_info['server_lan_port']}

    return res
