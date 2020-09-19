import unittest

# import s2d2_proj.sentinel_downloader.s2_downloader as s2_downloader

from sentinel_downloader import s2_downloader

import os
import logging
import sys

from django.conf import settings

import django

from pathlib import Path

BASE_DIR = settings.BASE_DIR

TEST_DIR = Path(BASE_DIR, "worker", "tests")

print(BASE_DIR)

from worker.tasks.download_s2 import download_s2

from common.utils import TaskStatus, ConfigFileProblem, ConfigValueMissing

from django.test import TestCase

logging.basicConfig(stream=sys.stderr, level=logging.WARNING)

django.setup()


class TestS2Downloader(TestCase):
    def setUp(self):
        pass

    # def test_config_init_no_config(self):

    #     with self.assertRaises(FileNotFoundError):
    #         s2_downloader.S2Downloader()

    # def test_config_init_bad_config(self):

    #     config_path = Path(TEST_DIR, "test_data", "config_bad.yaml")

    #     with self.assertRaises(ConfigFileProblem):
    #         s2_downloader.S2Downloader(path_to_config=config_path)

    # def test_config_init_bad_config2(self):

    #     config_path = Path(TEST_DIR, "test_data", "config_bad2.yaml")

    #     with self.assertRaises(ConfigFileProblem):
    #         s2_downloader.S2Downloader(path_to_config=config_path)

    # def test_config_init_extra_values(self):

    #     config_path = Path(TEST_DIR, "test_data", "config_extra.yaml")

    #     dl_obj = s2_downloader.S2Downloader(path_to_config=config_path)

    #     self.assertIsNotNone(dl_obj)

    # def test_config_init_missing_values(self):

    #     config_path = Path(TEST_DIR, "test_data", "config_missing.yaml")

    #     with self.assertRaises(ConfigValueMissing):
    #         s2_downloader.S2Downloader(path_to_config=config_path)

    # def test_config_init_good_config(self):

    #     config_path = Path(TEST_DIR, "test_data", "config.yaml")
    #     dl_obj = s2_downloader.S2Downloader(path_to_config=config_path)

    #     self.assertIsNotNone(dl_obj)

    # def test_build_url(self):
    #     s2_dl = s2_downloader.S2Downloader(
    #         path_to_config=Path(BASE_DIR, "test_config.yaml")
    #     )

    #     result = s2_dl.build_download_url("5ff875a1-bc41-4c43-adae-be4dfa03ad5f")
    #     # self.assertTrue(True)

    #     self.assertEquals(
    #         result,
    #         "https://scihub.copernicus.eu/dhus/odata/v1/Products('5ff875a1-bc41-4c43-adae-be4dfa03ad5f')/Nodes('S2A_MSIL1C_20190620T181921_N0207_R127_T12UXA_20190620T231306.SAFE')/Nodes('GRANULE')/Nodes('L1C_T12UXA_A020859_20190620T182912')/Nodes('IMG_DATA')/Nodes('T12UXA_20190620T181921_TCI.jp2')/$value",
    #     )

    # def test_download_tci(self):
    #     s2_dl = s2_downloader.S2Downloader(
    #         path_to_config=Path(BASE_DIR, "test_config.yaml")
    #     )

    #     result = s2_dl.download_tci(
    #         "6574b5fa-3898-4c9e-9c36-028193764211",
    #         Path(os.path.abspath(os.path.dirname(__file__)), "test_data"),
    #     )
    #     self.assertTrue(True)

    # def test_download_fullproduct(self):
    #     s2_dl = s2_downloader.S2Downloader(
    #         path_to_config=Path(BASE_DIR, "test_config.yaml")
    #     )

    #     result = s2_dl.download_fullproduct(
    #         "6574b5fa-3898-4c9e-9c36-028193764211",
    #         "S2A_MSIL1C_20190620T181921_N0207_R127_T12UXA_20190620T231306",
    #         Path(os.path.abspath(os.path.dirname(__file__)), "test_data"),
    #     )
    #     print(result)
    #     self.assertTrue(True)

    def test_download_fullproduct_celery(self):
        s2_dl = s2_downloader.S2Downloader(path_to_config=Path(BASE_DIR, "config.yaml"))

        result = s2_dl.download_fullproduct_callback(
            "6574b5fa-3898-4c9e-9c36-028193764211",
            "S2A_MSIL1C_20190620T181921_N0207_R127_T12UXA_20190620T231306",
            Path(os.path.abspath(os.path.dirname(__file__)), "test_data"),
            lambda x, y, z: print(f"{x} - {y} - {z}"),
        )
        print(result)
        self.assertTrue(True)

    def test_celery_task_download_s2(self):

        params = {
            "options": {
                "tile": "S2A_MSIL1C_20190620T181921_N0207_R127_T12UXA_20190620T231306",
                "ac": False,
                "ac_res": 10,
                "api_source": "ESA_SCIHUB",
                "entity_id": "na",
            }
        }

        # now_datetime = datetime.datetime.now()
        # job.status = JobStatusChoice.ASSIGNED.value
        # job.assigned = now_datetime
        # job.save()

        # result_list = start_job(
        #     params["tile"],
        #     atmospheric_correction=params["ac"],
        #     ac_res=params["ac_res"],
        #     api_source=params["api_source"],
        #     entity_id=params["entity_id"],
        #     celery_task=self,
        # )

        task = download_s2.s(params).apply()
        print(task)
        self.assertEqual(True, True)

    # def test_search_for_products_by_tile(self):
    #     """ Test search for products when an L2A product is available """
    #     # self, tiles, date_range, just_entity_ids=False

    #     # S2A_MSIL2A_20190904T102021_N0213_R065_T32UPV_20190904T140237

    #     s2_dl = s2_downloader.S2Downloader(
    #         path_to_config=Path(BASE_DIR, "test_config.yaml")
    #     )

    #     result = s2_dl.search_for_products_by_tile(["32UPV"], ("20190904", "20190905"),)
    #     print(result)

    # def test_search_for_products_by_tile_directly(self):

    #     s2_dl = s2_downloader.S2Downloader(
    #         path_to_config=Path(BASE_DIR, "test_config.yaml")
    #     )

    #     daterange = ("20190618", "20190619")
    #     print(daterange)
    #     result = s2_dl.search_for_products_by_tile_directly("20TMS", daterange)

    #     print(result)

    # def test_properties_file_creation(self):

    #     runner = gpt_runner.GPTRunner(
    #         self.product_path_arg,
    #         self.target_path_arg,
    #         self.path_to_graph_xml,
    #         self.arg_dict,
    #         1,
    #     )

    #     runner.generate_properties_file()

    #     self.assertTrue(Path("./process1.properties").is_file())

    # def test_run_processing(self):
    #     runner = gpt_runner.GPTRunner(
    #         self.product_path_arg,
    #         self.target_path_arg,
    #         self.path_to_graph_xml,
    #         self.arg_dict,
    #         1,
    #     )

    #     runner.generate_properties_file()
    #     date_start = datetime.datetime.now()
    #     runner.run_graph()
    #     date_end = datetime.datetime.now()

    #     time_elapsed = date_end - date_start
    #     d = {}
    #     d["hours"], rem = divmod(time_elapsed.seconds, 3600)
    #     d["minutes"], d["seconds"] = divmod(rem, 60)
    #     print(d)


if __name__ == "__main__":
    unittest.main()
