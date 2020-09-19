""" Helper script for creating a lot of jobs from a csv file


positional argument is the abs path to the csv containing the job infomation

for S1_Preprocessing the csv format is:

job_type, tile, preprocess_file, priority
S1_Preprocessing_v1, S1B_IW_GRDH_1SDV_20180930T002314_20180930T002339_012937_017E4F_9237, S1_v1_2_FullSceneTest_Orb_GM7x7_RDNormSig0wLIAfrDEM_TIF_CUBE.xml, 1

priority can be 1 - 3, job type depends on the revision of s2d2 that supports

"""

import csv
import argparse
import requests

from requests.auth import HTTPBasicAuth
# >>> requests.get('https://api.github.com/user', auth=HTTPBasicAuth('user', 'pass'))


server_url = 'zeus684440.agr.gc.ca:8080'

def parse_args():
    parser = argparse.ArgumentParser(description='Batch create jobs on there server')
    parser.add_argument('jobs_csv', type=str,
                        help='Parse a csv of jobs and create them on the server')

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = parse_args()

    with open(args.jobs_csv, 'r') as csv_file:
        reader = csv.DictReader(csv_file, delimiter=',')
        for row in reader:
            job_type = None
            
            job_type = row['job_type']
            
            if job_type == 'S1_Preprocessing_v1':
                s1_tile = None
                s1_preprocess = None
                priority = None
           
                s1_tile = row['tile']
                s1_preprocess = row['preprocess_file']
                priority = row['priority']

                label = f"S1_Preprocessing {s1_preprocess} {s1_tile}"
                
                command = f"python s2d2.py -tiles {s1_tile} -platform s1 -s1_sm IW -s1_pt GRD -s1_res H -s1_preprocess {s1_preprocess}"
                parameters = {
                    "s1_options": {
                        "res": "H",
                        "mode": "IW",
                        "tile": s1_tile,
                        "type": "GRD",
                        "preprocess_config": s1_preprocess
                    }
                }
            elif job_type == 'S1VirtualBandExtraction':
                tile = None
                
                priority = None
           
                tile = row['tile']
                priority = row['priority']

                label = f"S1VirtualBandExtraction {tile}"
                command = f"none"
                parameters = {
                    
                    "tile_name": tile,
                }

            json_args = {
                "job_type": job_type,
                "label": label,
                "command": command,
                "parameters": parameters,
                "priority": priority,
            }

            response = requests.post(f'http://{server_url}/jobs/',
                                     json=json_args,
                                     auth=HTTPBasicAuth('backup', '12341234'))

            print(response)
            print(response.status_code)
            print(response.json())

            # break