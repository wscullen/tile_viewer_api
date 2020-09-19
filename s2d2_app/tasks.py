from celery import task

import landsat_downloader.l8_downloader as l8_downloader
import sentinel_downloader.s2_downloader as s2_downloader

from django.conf import settings

from pathlib import Path

from PIL import Image

import os

import math


@task()
def download_fullrespreview(tile_dict, api_source):
    print("downloading the full res preview")

    highres_dir = Path(settings.MEDIA_ROOT, "highres_previews")
    print(tile_dict)

    if api_source == "usgs_ee":

        if tile_dict["platform_name"] == "Landsat-8":
            product_type = "FR_REFL"
        else:
            product_type = "FRB"

        l8_dl = l8_downloader.L8Downloader("", verbose=False)

        result = l8_dl.download_product(tile_dict, product_type, directory=highres_dir)

        print(result)

        file_name = result[2]

        result_justfilename = Path(file_name).name
        result_png_name = Path(result_justfilename).stem + ".png"

        print(result_justfilename)
        print(result_png_name)

        # nasa logo position and size
        # 7253 7462
        # 668 559
        # usgs logo position
        # 0 7671
        # 1276 379

        # Pillow code to make the nodata transparent
        image = Image.open(file_name)
        image = image.convert("RGBA")

        width, height = image.size

        usgs_logo_pos_x = 0
        usgs_logo_pos_y = height - 400
        usgs_logo_width = 1300
        usgs_logo_height = 400

        nasa_logo_pos_x = width - 900
        nasa_logo_pos_y = height - 750

        nasa_logo_width = 900
        nasa_logo_height = 750

        if tile_dict["platform_name"] == "Landsat-8":

            blackBoxNasa = Image.new(
                image.mode, (nasa_logo_width, nasa_logo_height), "#000"
            )
            blackBoxUSGS = Image.new(
                image.mode, (usgs_logo_width, usgs_logo_height), "#000"
            )

            image.paste(blackBoxNasa, (nasa_logo_pos_x, nasa_logo_pos_y))
            image.paste(blackBoxUSGS, (usgs_logo_pos_x, usgs_logo_pos_y))

        datas = image.getdata()

        newData = []
        for item in datas:
            if item[0] <= 20 and item[1] <= 20 and item[2] <= 20:
                newData.append((0, 0, 0, 0))
            else:
                newData.append(item)

        image.putdata(newData)

        image_half = image.resize((math.floor(width / 2), math.floor(height / 2)))
        image_quarter = image.resize((math.floor(width / 4), math.floor(height / 4)))

        image_half.save(Path(highres_dir, Path(result_justfilename).stem + "_half.png"))
        image_quarter.save(
            Path(highres_dir, Path(result_justfilename).stem + "_quar.png")
        )

        # image.save(Path(highres_dir, result_png_name))
        # once the PNG with transparancy is generated, remove original JPEG
        os.remove(file_name)

    elif api_source == "esa_scihub":
        s2_dl = s2_downloader.S2Downloader("")

        result = s2_dl.download_tci(tile_dict["entity_id"], highres_dir)

        file_name = result[2]

        result_justfilename = Path(file_name).name
        result_png_name = Path(result_justfilename).stem + ".png"

        print(result_justfilename)
        print(result_png_name)

        # nasa logo position and size
        # 7253 7462
        # 668 559
        # usgs logo position
        # 0 7671
        # 1276 379

        # Pillow code to make the nodata transparent
        image = Image.open(file_name)
        image = image.convert("RGBA")

        width, height = image.size

        datas = image.getdata()

        newData = []
        for item in datas:
            if item[0] <= 20 and item[1] <= 20 and item[2] <= 20:
                newData.append((0, 0, 0, 0))
            else:
                newData.append(item)

        image.putdata(newData)

        image_half = image.resize((math.floor(width / 2), math.floor(height / 2)))
        image_quarter = image.resize((math.floor(width / 4), math.floor(height / 4)))

        image_half.save(Path(highres_dir, Path(result_justfilename).stem + "_half.png"))
        image_quarter.save(
            Path(highres_dir, Path(result_justfilename).stem + "_quar.png")
        )

        # image.save(Path(highres_dir, result_png_name))
        # once the PNG with transparancy is generated, remove original JPEG
        os.remove(file_name)
