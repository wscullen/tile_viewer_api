import click

import requests

SERVER_URL = 'localhost:8090'

@click.command()
@click.option('--jobtype', nargs=1, type=click.Choice(['L8_AC', 'S2_AC']), required=True)
@click.argument('tiles', nargs=-1, required=True)
def submit_jobs(jobtype, tiles):
    print(jobtype)


    #   "url": "http://localhost:9090/jobs/ab7495df-bf0d-4ffe-a411-0a18f4fed80d/",
    #     "id": "ab7495df-bf0d-4ffe-a411-0a18f4fed80d",
    #     "submitted": "2019-09-19T23:48:30.131231Z",
    #     "assigned": "2019-09-20T00:00:32.629642Z",
    #     "completed": "2019-09-20T02:32:35.841648Z",
    #     "label": "L8Download LC08_L1TP_007028_20190905_20190917_01_T1",
    #     "command": "not used",
    #     "job_type": "L8Download",
    #     "parameters": {
    #         "options": {
    #             "ac": true,
    #             "tile": "LC08_L1TP_007028_20190905_20190917_01_T1"
    #         }
    #     },
    #     "priority": "3",
    #     "status": "C",
    #     "success": true,
    #     "result_message": null,
    #     "worker_id": null,
    #     "owner": "http://localhost:9090/users/1/"

    for tilename in tiles:
        print('Submitting job for tile:')
        print(tilename)
        job_type = "L8Download"
        priority = "3"
        label = f"L8Download {tilename}"
        command = f"na"
        parameters = {}
            "tile": tilename,
            "ac": True
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
                                    auth=HTTPBasicAuth('testuser', 'JumpMan85%'))

        print(response)
        print(response.status_code)
        if (response.status_code == 200):
            print('Job was submitted successfully')
            
        print(response.json())

if __name__ == '__main__':
    submit_jobs()