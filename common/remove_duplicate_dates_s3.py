"""
L1C Remove duplicates

Iterate over all the keys (one key for each archive)

Find the date for the product


S2B_MSIL1C_20180822T182909_N0206_R027_T11UPQ_20180822T225726.zip

date is 20180822T182909

find all keys with the same date

sort by the second date (processed date)

keep the most recent, delete the rest


L2A Remove duplicates

search by date using the wild card

delete all but the most recent using helper functions

"""

from django.conf import settings


from pathlib import Path
from datetime import datetime, date

from common.s3_utils import S3Utility


def sorting_function(x):

    if x.startswith("L1C"):
        return datetime(2000, 1, 1)
    else:
        x_fulldate = x.split("_")[-1][:-4]

        x_date_obj = datetime.strptime(x_fulldate, "%Y%m%dT%H%M%S")

        return x_date_obj


def sorting_function_key(x):
    print(x)
    x_fulldate = x[-1].split("_")[-1][:-5]

    x_date_obj = datetime.strptime(x_fulldate, "%Y%m%dT%H%M%S")

    return x_date_obj


def delete_duplicate_s2_l2a(
    config_dict,
    working_folder,
    bucket_name=f"s2-l2a-products{settings.BUCKET_SUFFIX}",
    dry_run=True,
):
    s3_helper = S3Utility(config_dict, working_folder)
    # paginator = s3_helper.s3_client.get_paginator("list_objects_v2")

    # all_objects = s3_helper.s3_client.list_objects_v2(Bucket="s2-l2a-products")
    # # print(all_objects)
    # print(len(all_objects["Contents"]))

    # key_total = 0
    # prev_key = None
    # try:
    #     for page in paginator.paginate(Bucket="s2-l2a-products", Prefix="tiles", PaginationConfig={'PageSize': 10}):

    #         # print(page["KeyCount"])

    #         if "Contents" in page.keys():
    #             for item in page["Contents"]:
    #                 prev_key = item
    #                 key_total += 1
    #                 # print(key_total)

    #         # if "ContinuationToken" in page.keys() and "NextContinuationToken"  not in page.keys():
    #         #     print('Found continuation token')
    #         #     print(page["ContinuationToken"])
    # except BaseException as e:
    #     print(e)
    #     print(prev_key)

    # print(key_total)
    product_key_list = []
    all_keys = []

    count = 0

    for item in s3_helper.iterate_bucket_items(bucket_name, "tiles"):

        key = item["Key"]
        # print(key)
        count += 1
        all_keys.append(key)
        key_split = key.split("/")
        # MTD_MSIL2A.xml
        if key_split[-1] == "MTD_MSIL2A.xml":
            print("Found a product")
            print(key_split[-2])
            product_key_list.append(key_split[:-1])

        # if count > 1000:
        #     break

    print(product_key_list)
    print(len(product_key_list))
    print(len(all_keys))
    total_deleted = []
    for product_key in product_key_list:

        current_product = product_key[-1]
        current_date = current_product.split("_")[2][:8]
        current_tile = current_product.split("_")[-2]

        print(f"Current product: {current_product}")
        print(f"Current date: {current_date}")
        print(f"Current tile: {current_tile}")

        print("=============================================")

        duplicate_key_list = []

        for prod_key in product_key_list:
            c_product = prod_key[-1]
            c_date = c_product.split("_")[2][:8]
            c_tile = c_product.split("_")[-2]
            print("----------")

            print(f"product: {c_product}")
            print(f"date: {c_date}")
            print(f"tile: {c_tile}")

            if c_tile == current_tile and c_date == current_date:
                print("Found a duplicate, adding to the list")
                duplicate_key_list.append(prod_key)

        if len(duplicate_key_list) > 1:

            print("DUPLICATE LIST IS LONGER THAN 1 (NOT JUST A DUPLICATE WITH ITSELF")
            print(duplicate_key_list)
            duplicate_key_list.sort(key=sorting_function_key, reverse=True)
            print(duplicate_key_list)
            del duplicate_key_list[0]

            print(duplicate_key_list)
            deleted_items = []
            for item in duplicate_key_list:
                print("Deleting duplicate item")
                print(item)
                deleted_items.append(item)

                if not dry_run:
                    print("This is not A DRY RUN, actually removing!")
                    # s3_helper.s3_client.delete_object(Bucket=bucket_name, Key=f)
                    s3_helper.s3_resource.Bucket(bucket_name).objects.filter(
                        Prefix=str(Path(*item))
                    ).delete()
                else:
                    print("This is a dry run, but this would have been deleted:")
                    print(Path(*item))

            print("DELETED ITEMS")
            total_deleted.extend(deleted_items)

            if not dry_run:
                print("ACTUALLY DELETED (NOT A DRY RUN)")

            for item in deleted_items:
                print(item)
    if dry_run:
        print("Nothing was deleted because this was  dry run")

    print(f"Total deleted items: {len(total_deleted)}")
    for item in total_deleted:
        print("/".join(item))


def delete_duplicate_s2_l1c(
    config_dict,
    working_folder,
    bucket_name=f"s2-l1c-archive{settings.BUCKET_SUFFIX}",
    dry_run=True,
):

    s3_helper = S3Utility(config_dict, working_folder)
    total_deleted = []
    for item in s3_helper.iterate_bucket_items(bucket_name, ""):

        duplicate_key_list = []
        current_key = item["Key"]

        print(current_key)

        if current_key.startswith("L1C"):
            print("this is a usgs product")
            # L1C_T11UNV_A019987_20190420T185731.zip
            sensing_date_to_match = current_key.split("_")[3][:-4]
            print(sensing_date_to_match)
            tile_to_match = current_key.split("_")[1]
        else:
            # S2B_MSIL1C_20190707T153819_N0207_R011_T19UDQ_20190707T191408.zip
            sensing_date_to_match = current_key.split("_")[2]
            tile_to_match = current_key.split("_")[5]

        for item in s3_helper.iterate_bucket_items(bucket_name, ""):
            temp_key = item["Key"]
            if temp_key.startswith("L1C"):
                print("this is a usgs product")
                # L1C_T11UNV_A019987_20190420T185731.zip
                sensing_date = temp_key.split("_")[3][:-4]
                print(sensing_date)
                tile = temp_key.split("_")[1]
            else:
                sensing_date = temp_key.split("_")[2]
                tile = temp_key.split("_")[5]

            if sensing_date == sensing_date_to_match and tile_to_match == tile:
                duplicate_key_list.append(temp_key)

        if len(duplicate_key_list) > 1:

            duplicate_key_list.sort(key=sorting_function, reverse=True)
            # sorting_function(duplicate_key_list[0], duplicate_key_list[1])

            del duplicate_key_list[0]

            print(duplicate_key_list)

            deleted_items = []
            for item in duplicate_key_list:
                print("Deleting duplicate item")
                print(item)
                deleted_items.append(item)

                if not dry_run:
                    print("This is not A DRY RUN, actually removing!")
                    s3_helper.s3_client.delete_object(Bucket=bucket_name, Key=item)

            print("DELETED ITEMS")

            if not dry_run:
                print("ACTUALLY DELETED (NOT A DRY RUN)")

            for item in deleted_items:
                print(item)

            total_deleted.extend(deleted_items)

    if dry_run:
        print("Nothing was deleted because this was  dry run")

    print(f"Total deleted items: {len(total_deleted)}")
    for item in total_deleted:
        print(item)


if __name__ == "__main__":
    config_dict = {
        "S3_URL": "http://zeus684440.agr.gc.ca:9000",
        "S3_ACCESS_KEY": "5ALMSEL6NYALHE1ASLKL",
        "S3_SECRET_KEY": "R6G+MxluzB39gIDmmPf+63iIqRpMmhTIHkOBiuKV",
        "S3_REGION": "us-east-1",
    }

    # delete_duplicate_s2_l1c(config_dict, Path(".", "working_folder"), dry_run=True)
    delete_duplicate_s2_l2a(config_dict, Path(".", "working_folder"), dry_run=False)
