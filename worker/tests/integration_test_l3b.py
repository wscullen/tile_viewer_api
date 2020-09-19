print("help")

from worker.tasks import sen2agri_tasks

print("import fails")

# @override_settings(CELERY_TASK_ALWAYS_EAGER=True)
def test_sen2agri_tasks_l3b():
    from worker.tasks import sen2agri_tasks_l3b

    imagery_list_example = {
        "sentinel2": {
            "20190613": [
                "S2A_MSIL1C_20190613T182921_N0207_R027_T12UUA_20190613T220508"
            ],
            "20190616": [],
            "20190618": [
                "S2B_MSIL1C_20190618T182929_N0207_R027_T12UUA_20190618T220500"
            ],
            "20190621": [],
            "20190623": [],
            "20190626": [],
            "20190628": [
                "S2B_MSIL1C_20190628T182929_N0207_R027_T12UUA_20190628T221748"
            ],
            "20190701": [],
            "20190703": [
                "S2A_MSIL1C_20190703T182921_N0207_R027_T12UUA_20190703T220857"
            ],
            "20190718": [
                "S2B_MSIL1C_20190718T182929_N0208_R027_T12UUA_20190718T220349"
            ],
            # ],
            # "20190723": [
            #     "S2A_MSIL1C_20190723T182921_N0208_R027_T12UUA_20190723T220633"
            # ],
            # "20190728": [
            #     "S2B_MSIL1C_20190728T182929_N0208_R027_T12UUA_20190728T215921"
            # ],
            # "20190807": [
            #     "S2B_MSIL1C_20190807T182929_N0208_R027_T12UUA_20190807T220535"
            # ],
        }
    }
    task = sen2agri_tasks_l3b.start_l3b_job(
        imagery_list_example, "Test AOI Name", "monodate", "job_id_0000"
    )
    print(task)


test_sen2agri_tasks_l3b()


if __name__ == "__main__":
    print("please work")
