## JOB DEFINITION EXAMPLES

## S1 Example Job

Label:
S1 Download with Preprocessing
Command:
python s2d2.py -tiles S1B_IW_GRDH_1SDV_20180504T001446_20180504T001511_010764_013ABB_0FBB -platform s1 -s1_sm IW -s1_pt GRD -s1_res H -s1_preprocess default
Job type:
DownloadPreprocess
Parameters:
{"platform": "s1", "tile": "S1B_IW_GRDH_1SDV_20180504T001446_20180504T001511_010764_013ABB_0FBB", "s1_options": {"mode": "IW", "type": "GRD", "res": "H", "preprocess_config": "gamma7x7_norlim_sigma0_uint16"}}

## S2 Example Job with atmospheric correction

Label:
S2 Download with Sen2Cor AtmosCor
Command:
python s2d2.py -tiles L1C_T10UCB_A017399_20181021T192430 -c 100 -cli -platform s2 -ac 10
Job type:
S2_Download_S2CAtmosCor
Parameters:
{
"platform": "s2",
"tile": "L1C_T10UCB_A017399_20181021T192430",
"s2_options": {
"res": 10}
}
