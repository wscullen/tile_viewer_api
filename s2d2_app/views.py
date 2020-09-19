from django.shortcuts import render

from django.utils.crypto import get_random_string

from django.conf import settings

from shapely.wkt import loads as wkt_loads
from shapely.geometry import mapping

from rest_framework.renderers import JSONRenderer
from rest_framework.response import Response

import json

import requests
from requests.auth import HTTPBasicAuth
import os
import uuid

from .tasks import download_fullrespreview

from PIL import Image
from PIL import ImageEnhance

import logging

# Create your views here.
from django.contrib.auth.models import User, Group
from rest_framework import viewsets
from s2d2_app.serializers import UserSerializer, GroupSerializer

from pathlib import Path
from django.conf import settings

from datetime import datetime
import datetime as dt

import spatial_ops.grid_intersect as grid_intersect
import spatial_ops.utils as spatial_utils
import landsat_downloader.l8_downloader as l8_downloader
from sentinel_downloader import s2_downloader

module_logger = logging.getLogger(__name__)


class UserViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows users to be viewed or edited.
    """

    queryset = User.objects.all().order_by("-date_joined")
    serializer_class = UserSerializer


class GroupViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows groups to be viewed or edited.
    """

    queryset = Group.objects.all()
    serializer_class = GroupSerializer


from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import authentication, permissions
from django.contrib.auth.models import User


class ListUsers(APIView):
    """
    View to list all users in the system.

    * Requires token authentication.
    * Only admin users are able to access this view.
    """

    # authentication_classes = (authentication.TokenAuthentication,)
    # permission_classes = (permissions.IsAdminUser,)

    def get(self, request, format=None):
        """
        Return a list of all users.
        """
        usernames = [user.username for user in User.objects.all()]
        return Response(usernames)


from django.shortcuts import render
from django.conf import settings
from django.core.files.storage import FileSystemStorage


def simple_upload(request):
    if request.method == "POST" and request.FILES["myfile"]:
        myfile = request.FILES["myfile"]
        fs = FileSystemStorage()
        filename = fs.save(myfile.name, myfile)
        uploaded_file_url = fs.url(filename)
        return render(
            request,
            "s2d2_app/file_upload.html",
            {"uploaded_file_url": uploaded_file_url},
        )
    return render(request, "s2d2_app/file_upload.html")


def multi_upload(request):
    if request.method == "POST" and request.FILES.getlist("myfiles"):
        files_urls = []

        shapefile_uploaded = None

        random_rename_string = get_random_string(length=8)

        for afile in request.FILES.getlist("myfiles"):
            fs = FileSystemStorage()

            # check if a file with the same name already exists
            full_path = Path(settings.MEDIA_ROOT, afile.name)
            module_logger.debug(afile.name)
            if full_path.exists():
                filename = (
                    Path(afile.name).stem
                    + random_rename_string
                    + Path(afile.name).suffix
                )
            else:
                filename = afile.name

            filename = fs.save(filename, afile)
            uploaded_file_url = fs.url(filename)

            if Path(filename).suffix == ".shp":
                module_logger.debug(uploaded_file_url)
                module_logger.debug(filename)
                shapefile_uploaded = Path(settings.MEDIA_ROOT, filename)

                print(shapefile_uploaded)

            files_urls.append(uploaded_file_url)

        if shapefile_uploaded:
            wkt_footprint = grid_intersect.get_wkt_from_shapefile(
                str(shapefile_uploaded)
            )

            mgrs_list = grid_intersect.find_mgrs_intersection(wkt_footprint)
            wrs_list = grid_intersect.find_wrs_intersection(wkt_footprint)

            # config_path
            # landsat_downloader query here
            # search_for_products_by_tile
            # search_for_products_by_tile(self, dataset_name, tile_list, query_dict, just_entity_ids=False, write_to_csv=False, detailed=False):

            date_start = datetime.strptime("20180601", "%Y%m%d")
            date_end = datetime.strptime("20180630", "%Y%m%d")

            arg_list = {"date_start": date_start, "date_end": date_end}

            arg_list["cloud_percent"] = 100

            config_path = Path(
                Path(__file__).absolute().parent.parent,
                "landsat_downloader",
                "config.json",
            )
            module_logger.debug(config_path)
            downloader = l8_downloader.L8Downloader(config_path, verbose=False)
            search_results = downloader.search_for_products_by_tile(
                "SENTINEL_2A", mgrs_list, arg_list, detailed=True
            )

            module_logger.debug(search_results)

            return render(
                request,
                "s2d2_app/multi_file_upload.html",
                {
                    "uploaded_file_url_list": files_urls,
                    "wkt_footprint": wkt_footprint,
                    "mgrs_list": mgrs_list,
                    "wrs_list": wrs_list,
                },
            )

        else:
            return render(
                request,
                "s2d2_app/multi_file_upload.html",
                {"uploaded_file_url_list": files_urls},
            )
    return render(request, "s2d2_app/multi_file_upload.html")


def download_file(url):
    local_filename = url.split("/")[-1]
    local_filename_png = local_filename.split(".")[0] + ".png"
    low_res_dir = Path(settings.MEDIA_ROOT, "lowres_previews")
    full_path = Path(low_res_dir, local_filename)
    full_path_png = Path(low_res_dir, local_filename_png)

    module_logger.debug("download_file")
    module_logger.debug(url)

    if not low_res_dir.is_dir():
        module_logger.debug("lowres_previews dir is missing, creating...")
        os.mkdir(low_res_dir)

    if full_path.exists():
        module_logger.debug("file already exists")
        module_logger.debug(full_path)
        return local_filename

    if full_path_png.exists():
        module_logger.debug("file already exists")
        module_logger.debug(full_path_png)
        return local_filename_png

    try:
        # NOTE the stream=True parameter below
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(full_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
                        # f.flush()

            # Pillow code to make the nodata transparent
            image = Image.open(full_path)
            image = image.convert("RGBA")
            datas = image.getdata()

            newData = []
            for item in datas:
                if item[0] <= 20 and item[1] <= 20 and item[2] <= 20:
                    newData.append((0, 0, 0, 0))
                else:
                    newData.append(item)

            image.putdata(newData)

            image.save(Path(low_res_dir, local_filename_png))
            # once the PNG with transparancy is generated, remove original JPEG
            os.remove(full_path)

    except BaseException as e:
        module_logger.error("Downloading lowres preview image failed...")
        module_logger.error(e)
        return None
    else:
        return local_filename_png


def download_file_esa(url, name):
    local_filename = name + ".jpg"
    local_filename_png = name + ".png"
    low_res_dir = Path(settings.MEDIA_ROOT, "lowres_previews")
    full_path = Path(low_res_dir, local_filename)
    full_path_png = Path(low_res_dir, local_filename_png)

    esa_config = settings.ESA_SCIHUB_CONFIG
    module_logger.debug("download_file_esa")
    module_logger.debug(esa_config)

    if not low_res_dir.is_dir():
        module_logger.debug("lowres_previews dir is missing, creating...")
        os.mkdir(low_res_dir)

    if full_path.exists():
        module_logger.debug("file already exists")
        module_logger.debug(full_path)
        return local_filename

    if full_path_png.exists():
        module_logger.debug("file already exists")
        module_logger.debug(full_path_png)
        return local_filename_png

    try:
        # NOTE the stream=True parameter below
        with requests.get(
            url, stream=True, auth=HTTPBasicAuth(esa_config["USER"], esa_config["PASS"])
        ) as r:
            r.raise_for_status()
            with open(full_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
                        # f.flush()

            # Pillow code to make the nodata transparent
            image = Image.open(full_path)
            image = image.convert("RGBA")
            datas = image.getdata()

            newData = []
            for item in datas:
                if item[0] <= 20 and item[1] <= 20 and item[2] <= 20:
                    newData.append((0, 0, 0, 0))

                # else:
                #     boostedValue = [item[0], item[1], item[2], 255]
                # if item[2] > 100:
                #     boostedValue[0] = 255
                #     boostedValue[1] = 0
                #     boostedValue[2] = 0
                #     boostedValue[3] = 255
                # # else:
                # #     boostedValue[0] *= 1.05
                # #     boostedValue[1] *= 1.3

                # newData.append((int(boostedValue[0]), int(boostedValue[1]), int(boostedValue[2]), int(boostedValue[3])))
                else:
                    newData.append(item)

            image.putdata(newData)
            image = ImageEnhance.Color(image).enhance(1.2)
            image = ImageEnhance.Contrast(image).enhance(1.3)
            image = ImageEnhance.Brightness(image).enhance(1.2)

            # for i in range(8):
            #     factor = i / 4.0
            #     enhancer.enhance(factor).show("Sharpness %f" % factor)

            image.save(Path(low_res_dir, local_filename_png))
            # once the PNG with transparancy is generated, remove original JPEG
            os.remove(full_path)

    except BaseException as e:
        module_logger.error("Downloading lowres preview image failed...")
        module_logger.error(e)
        return None
    else:
        return local_filename_png


def create_geojson_wrs_overlay(wrs_overlay_list):
    """Given a tuple of pathrow and geometries, create a geojson feature collection

          {
    "type": "FeatureCollection",
    "features": [
        {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [0, 0]
        },
        "properties": {
            "name": "null island"
        }
        }
    ]
    }

    """
    feature_collection = {"type": "FeatureCollection", "features": []}

    for wrs_tuple in wrs_overlay_list:
        # Convert to a shapely.geometry.polygon.Polygon object
        wkt_string = wrs_tuple[1]
        fp_temp = wkt_loads(str(wkt_string))

        module_logger.debug(f"fp_temp: {fp_temp}")
        fp_output = mapping(fp_temp)
        module_logger.debug(f"fp_output: {fp_output}")

        feature = {
            "type": "Feature",
            "geometry": fp_output,
            "bbox": fp_temp.bounds,
            "id": str(uuid.uuid4()),
            "properties": {"name": wrs_tuple[0]},
        }

        feature_collection["features"].append(feature)

    return feature_collection


def create_geojson_mgrs_overlay(mgrs_overlay_list):
    """Given a tuple of pathrow and geometries, create a geojson feature collection

          {
    "type": "FeatureCollection",
    "features": [
        {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [0, 0]
        },
        "properties": {
            "name": "null island"
        }
        }
    ]
    }

    """
    feature_collection = {"type": "FeatureCollection", "features": []}

    for mgrs_tuple in mgrs_overlay_list:
        # Convert to a shapely.geometry.polygon.Polygon object
        wkt_string = mgrs_tuple[1]
        fp_temp = wkt_loads(str(wkt_string))

        module_logger.debug(f"fp_temp: {fp_temp}")
        fp_output = mapping(fp_temp)
        module_logger.debug(f"fp_output: {fp_output}")

        feature = {
            "type": "Feature",
            "geometry": fp_output,
            "bbox": fp_temp.bounds,
            "id": str(uuid.uuid4()),
            "properties": {"name": mgrs_tuple[0]},
        }

        feature_collection["features"].append(feature)

    return feature_collection


def create_geojson_feature(feature_dict):
    """From a python dictionary, construct a geojson version of it
    acquisition_end:
    "2018-07-01T18:53:38.100000"
    acquisition_start:
    "2018-07-01T18:42:30.660000"
    api_source:
    "usgs_ee"
    cloud_percent:
    "65.4908"
    dataset_name:
    "SENTINEL_2A"
    detailed_metadata:
    Array[51]
    download_source:
    null
    entity_id:
    "4459209"
    footprint:
    "POLYGON ((-113.76410 49.53173, -112.24722 49.55803, -112.27308 50.54533, -113.82133 50.51810, -113.76410 49.53173))"
    manual_bulkorder_url:
    "n/a"
    manual_download_url:
    "https://earthexplorer.usgs.gov/download/external/options/SENTINEL_2A/4459209/INVSVC/"
    manual_product_url:
    "https://earthexplorer.usgs.gov/order/process?dataset_name=SENTINEL_2A&ordered=4459209&node=INVSVC"
    mbr:
    "POLYGON((-113.82133 50.54533, -112.24722 50.54533, -112.24722 49.53173, -113.82133 49.53173, -113.82133 50.54533))"
    metadata_url:
    "https://earthexplorer.usgs.gov/metadata/xml/10880/4459209/"
    mgrs:
    "T12UUA"
    name:
    "L1C_T12UUA_A015797_20180701T184230"
    pathrow:
    "n/a "
    platform_name:
    "Sentinel-2"
    preview_url:
    "http://hal678772.agr.gc.ca:8000/media/lowres_previews/L1C_T12UUA_A015797_20180701T184230.jpg"
    sat_name:
    "SENTINEL-2A"
    summary:
    "Entity ID: L1C_T12UUA_A015797_20180701T184230, Acquisition Date: 01-JUL-18, Start Date: 01-JUL-18, End Date: 01-JUL-18"
    uuid:
    "4459209"
    vendor_name:
    "S2A_MSIL1C_20180701T183921_N0206_R070_T12UUA_20180701T222155"
    """
    module_logger.debug("helloooooooooooooooooooooooooooo")
    module_logger.debug(feature_dict)
    footprint = feature_dict["footprint"]
    module_logger.debug(footprint)
    mbr = feature_dict["mbr"]
    module_logger.debug(mbr)
    module_logger.debug("shapely?")
    # Convert to a shapely.geometry.polygon.Polygon object
    fp_temp = wkt_loads(str(footprint))
    module_logger.debug(fp_temp)
    fp_output = mapping(fp_temp)
    module_logger.debug(fp_output)
    mbr_temp = wkt_loads(str(mbr))
    module_logger.debug(mbr_temp)
    mbr_output = mapping(mbr_temp)
    module_logger.debug(mbr_output)
    geojson_version = {
        "type": "Feature",
        "geometry": fp_output,
        "bbox": mbr_temp.bounds,
        "id": str(uuid.uuid4()),
        "properties": {
            "acquisition_start": feature_dict["acquisition_start"].isoformat(),
            "acquisition_end": feature_dict["acquisition_end"].isoformat(),
            "api_source": feature_dict["api_source"],
            "cloud_percent": feature_dict["cloud_percent"],
            "dataset_name": feature_dict["dataset_name"],
            "entity_id": feature_dict["entity_id"],
            "manual_bulkorder_url": feature_dict["manual_bulkorder_url"],
            "manual_download_url": feature_dict["manual_download_url"],
            "manual_product_url": feature_dict["manual_product_url"],
            "metadata_url": feature_dict["metadata_url"],
            "mgrs": feature_dict["mgrs"],
            "name": feature_dict["name"],
            "pathrow": feature_dict["pathrow"],
            "platform_name": feature_dict["platform_name"],
            "preview_url": feature_dict["preview_url"],
            "sat_name": feature_dict["sat_name"],
            "summary": feature_dict["summary"],
            "vendor_name": feature_dict["vendor_name"],
        },
    }
    module_logger.debug(geojson_version)

    return geojson_version


def create_geojson_feature_esa(feature_dict):
    """From a python dictionary, construct a geojson version of it

    """

    footprint = feature_dict["footprint"]
    # mbr = feature_dict["mbr"]

    # Convert to a shapely.geometry.polygon.Polygon object
    actual_temp = wkt_loads(footprint)

    module_logger.debug("shapeley!!!!!")
    # json.dumps(g2)
    module_logger.debug("footprint")
    module_logger.debug(actual_temp)
    module_logger.debug(actual_temp.geom_type)

    if actual_temp.geom_type == "MultiPolygon":
        # do multipolygon things.
        actual_polygon = list(actual_temp)[0]

    elif actual_temp.geom_type == "Polygon":
        # do polygon things.
        actual_polygon = actual_temp
    else:
        # raise IOError('Shape is not a polygon.')
        raise IOError("Invalid footprint geometry (Not a polygon or multipolygon).")

    actual_output = mapping(actual_polygon)
    module_logger.debug(actual_output)

    geojson_version = {
        "type": "Feature",
        "geometry": actual_output,
        "bbox": actual_polygon.bounds,
        "id": str(uuid.uuid4()),
        "properties": {
            "acquisition_start": feature_dict["beginposition"].isoformat(),
            "acquisition_end": feature_dict["endposition"].isoformat(),
            "api_source": feature_dict["api_source"],
            "cloud_percent": int(feature_dict["cloudcoverpercentage"]),
            "dataset_name": feature_dict["platformname"],
            "entity_id": feature_dict["uuid"],
            "manual_bulkorder_url": None,
            "manual_download_url": None,
            "manual_product_url": None,
            "metadata_url": feature_dict["link"],
            "mgrs": feature_dict["tileid"],
            "name": feature_dict["name"],
            "pathrow": None,
            "platform_name": feature_dict["platformname"],
            "preview_url": feature_dict["link_icon"],
            "sat_name": feature_dict["platformserialidentifier"],
            "summary": feature_dict["summary"],
            "vendor_name": feature_dict["title"],
            "footprint_data": actual_output,
        },
    }

    return geojson_version


def parseVisualizationShapefiles(visualization_shapefiles):
    """Separate the files into groups for each prefix number

    Validate, save, get path to the .shp for each group
    """

    files = {}
    shapefile_paths = []

    for f in visualization_shapefiles:
        f_split = f.name.split("+")
        file_name = f_split[1]
        file_index = f_split[0]

        if file_index not in files.keys():
            files[file_index] = []
            files[file_index].append((file_name, f))
        else:
            files[file_index].append((file_name, f))

    module_logger.info(files)

    for idx, file_list in files.items():
        shapefile_uploaded = None
        random_rename_string = get_random_string(length=8)

        # Make sure that all the required shapefiles are there
        file_ext_name_list = [Path(f[0]).suffix for f in file_list]
        shapefile_ext_list = [".shp", ".shx", ".dbf", ".prj"]
        missing_ext_list = []
        for ext in shapefile_ext_list:
            if ext not in file_ext_name_list:
                module_logger.debug(f"missing {ext} file")
                module_logger.debug(
                    f"files with these extensions found: {file_ext_name_list}"
                )
                missing_ext_list.append(ext)

        if missing_ext_list:
            return Response(
                {
                    "error": f'Missing required files for shapefile ({", ".join(missing_ext_list)})'
                }
            )

        for afile in file_list:
            module_logger.info(afile)
            file_name = afile[0]
            f = afile[1]
            module_logger.info("hello")
            module_logger.info(file_name)

            fs = FileSystemStorage()

            # check if a file with the same name already exists
            full_path = Path(settings.MEDIA_ROOT, file_name)

            if full_path.exists():
                filename = (
                    Path(file_name).stem + random_rename_string + Path(file_name).suffix
                )
            else:
                filename = file_name

            filename = fs.save(filename, f)

            uploaded_file_url = fs.url(filename)
            module_logger.info(Path(filename).suffix)

            if Path(filename).suffix == ".shp":
                module_logger.info(uploaded_file_url)
                module_logger.info(filename)
                shapefile_uploaded = Path(settings.MEDIA_ROOT, filename)
                shapefile_paths.append((Path(file_name).stem + idx, shapefile_uploaded))

                module_logger.info(shapefile_uploaded)

    return shapefile_paths


class SubmitAOI(APIView):
    """
    Start a query based on the area in a shapefile
    """

    # authentication_classes = (authentication.TokenAuthentication,)
    # permission_classes = (permissions.IsAdminUser,)
    renderer_classes = (JSONRenderer,)

    def post(self, request, format=None):
        """
        Standard
        """

        HOSTNAME = request.get_host()
        module_logger.debug("hello")
        module_logger.info(request.FILES)

        shapefiles = request.FILES.getlist("shapefiles")
        module_logger.debug(shapefiles)

        visualization_shapefiles = request.FILES.getlist("visualizationShapefiles")
        module_logger.info(visualization_shapefiles)

        if request.FILES.getlist("shapefiles"):

            files_urls = []
            shapefile_uploaded = None
            random_rename_string = get_random_string(length=8)

            # Make sure that all the required shapefiles are there
            file_ext_name_list = [
                Path(f.name).suffix for f in request.FILES.getlist("shapefiles")
            ]
            shapefile_ext_list = [".shp", ".shx", ".dbf", ".prj"]
            missing_ext_list = []
            for ext in shapefile_ext_list:
                if ext not in file_ext_name_list:
                    module_logger.debug(f"missing {ext} file")
                    module_logger.debug(
                        f"files with these extensions found: {file_ext_name_list}"
                    )
                    missing_ext_list.append(ext)

            if missing_ext_list:
                return Response(
                    {
                        "error": f'Missing required files for shapefile ({", ".join(missing_ext_list)})'
                    }
                )

            module_logger.debug(request.FILES.getlist("shapefiles"))

            for afile in request.FILES.getlist("shapefiles"):

                module_logger.debug(afile.name)

                fs = FileSystemStorage()

                # check if a file with the same name already exists
                full_path = Path(settings.MEDIA_ROOT, afile.name)

                if full_path.exists():
                    filename = (
                        Path(afile.name).stem
                        + random_rename_string
                        + Path(afile.name).suffix
                    )
                else:
                    filename = afile.name

                filename = fs.save(filename, afile)

                uploaded_file_url = fs.url(filename)

                if Path(filename).suffix == ".shp":
                    module_logger.debug(uploaded_file_url)
                    module_logger.debug(filename)
                    shapefile_uploaded = Path(settings.MEDIA_ROOT, filename)

                    module_logger.debug(shapefile_uploaded)

                files_urls.append(uploaded_file_url)

            if shapefile_uploaded:
                visualization_wkt_list = []

                # Handle visualization shapefile conversion
                if request.FILES.getlist("visualizationShapefiles"):
                    module_logger.info("visualization shapefiles uploaded")
                    shapefile_paths = parseVisualizationShapefiles(
                        request.FILES.getlist("visualizationShapefiles")
                    )
                    module_logger.info("Shapefile paths:")
                    module_logger.info(shapefile_paths)

                    if shapefile_paths:
                        for shapefile in shapefile_paths:
                            wkt = grid_intersect.get_wkt_from_shapefile(
                                str(shapefile[1])
                            )
                            visualization_wkt_list.append(
                                {"name": shapefile[0], "wkt": wkt}
                            )

                    module_logger.info(visualization_wkt_list)

                wkt_footprint = grid_intersect.get_wkt_from_shapefile(
                    str(shapefile_uploaded)
                )

                module_logger.info("Finding MGRS intersection list...")
                mgrs_list = grid_intersect.find_mgrs_intersection(wkt_footprint)
                module_logger.info("Finding WRS intersection list...")
                wrs_list = grid_intersect.find_wrs_intersection(wkt_footprint)

                wrs_wkt_geometry = []

                for wrs in wrs_list:
                    wkt = grid_intersect.get_wkt_for_wrs_tile(wrs)
                    wrs_wkt_geometry.append((wrs, wkt))

                module_logger.debug("WRS AND WKT")
                module_logger.debug(wrs_wkt_geometry)

                wrs_geojson = create_geojson_wrs_overlay(wrs_wkt_geometry)
                module_logger.debug(wrs_geojson)

                mgrs_wkt_geometry = []

                for mgrs in mgrs_list:
                    wkt = grid_intersect.get_wkt_for_mgrs_tile(mgrs)
                    mgrs_wkt_geometry.append((mgrs, wkt))

                module_logger.debug("MGRS AND WKT")
                module_logger.debug(mgrs_wkt_geometry)

                mgrs_geojson = create_geojson_mgrs_overlay(mgrs_wkt_geometry)
                module_logger.debug(mgrs_geojson)

                # config_path
                # landsat_downloader query here
                # search_for_products_by_tile
                # search_for_products_by_tile(self, dataset_name, tile_list, query_dict, just_entity_ids=False, write_to_csv=False, detailed=False):

                aoi_fields = request.data

                module_logger.debug(aoi_fields)
                date_start = datetime.strptime(aoi_fields["startDate"], "%Y%m%d")
                date_end = datetime.strptime(aoi_fields["endDate"], "%Y%m%d")

                arg_list = {"date_start": date_start, "date_end": date_end}

                arg_list["cloud_percent"] = 100
                arg_list["collection_category"] = ["T1", "T2"]

                # {'fieldId': 20510, 'name': 'Collection Category', 'fieldLink': 'https://lta.cr.usgs.gov/DD/landsat_dictionary.html#collection_category', 'valueList': [{'value': None, 'name': 'All'}, {'value': 'T1', 'name': 'Tier 1'}, {'value': 'T2', 'name': 'Tier 2'}, {'value': 'RT', 'name': 'Real-Time'}]},

                module_logger.debug(arg_list)
                config_path = Path(settings.BASE_DIR, "config.yaml")
                module_logger.debug(config_path)

                search_results = {}
                platforms = aoi_fields["platforms"].split(",")
                module_logger.debug(platforms)
                for platform in platforms:
                    if platform == "sentinel2":

                        s2_dl = s2_downloader.S2Downloader(config_path)
                        s2_end_date = date_end + dt.timedelta(days=1)
                        s2_results = s2_dl.search_for_products_by_tile(
                            mgrs_list, (date_start, s2_end_date), product_type="L1C"
                        )
                        module_logger.debug(s2_results)

                        module_logger.debug("scihub sentinel results ")

                        search_results[platform] = []
                        for key in s2_results.keys():
                            module_logger.debug(key)
                            product_dict = s2_results[key]
                            module_logger.debug(product_dict)
                            if "tileid" not in product_dict.keys():
                                product_dict["tileid"] = product_dict["title"].split(
                                    "_"
                                )[5][1:]

                            wkt_string = str(product_dict["footprint"])
                            module_logger.debug(wkt_string)

                            data_footprint = wkt_loads(wkt_string)

                            module_logger.debug(data_footprint.geom_type)

                            if data_footprint.geom_type == "MultiPolygon":
                                # do multipolygon things.
                                actual_polygon = list(data_footprint)[0]
                            elif data_footprint.geom_type == "Polygon":
                                # do polygon things.
                                actual_polygon = data_footprint
                            else:
                                # raise IOError('Shape is not a polygon.')
                                raise IOError(
                                    "Invalid footprint geometry (Not a polygon or multipolygon)."
                                )

                            module_logger.debug(actual_polygon)
                            # check if the valid data footprint actually intersects our area of interest
                            data_intersect = spatial_utils.polygons_intersect(
                                wkt_footprint, str(actual_polygon)
                            )

                            module_logger.debug(data_intersect)

                            if data_intersect:
                                product_dict[
                                    "footprint"
                                ] = grid_intersect.get_wkt_for_mgrs_tile(
                                    product_dict["tileid"]
                                )
                                module_logger.debug(product_dict)
                                product_dict["name"] = product_dict["title"]
                                product_dict["acquisition_start"] = product_dict[
                                    "beginposition"
                                ]
                                product_dict["acquisition_end"] = product_dict[
                                    "endposition"
                                ]
                                # title_parts = product_dict['title'].split('_')
                                # product_dict['usgs_name'] = f'{title_parts[1][3:]}_{title_parts[5]}_A{str(product_dict["orbitnumber"]).zfill(6)}_{title_parts[2]}'
                                product_dict["espg_code"] = 4326
                                product_dict["cloud_percent"] = str(
                                    product_dict["cloudcoverpercentage"]
                                )
                                product_dict["geojson"] = create_geojson_feature_esa(
                                    product_dict
                                )

                                # Steps
                                # Download preview image to media folder
                                # update low res preview url for each tile.
                                module_logger.debug(
                                    "trying to download lowres preview url"
                                )
                                local_filename = download_file_esa(
                                    product_dict["link_icon"], product_dict["title"]
                                )
                                module_logger.debug(HOSTNAME)

                                if local_filename:
                                    module_logger.debug(
                                        f"http://{HOSTNAME}/media/lowres_previews/{local_filename}"
                                    )
                                    product_dict[
                                        "preview_url"
                                    ] = f"http://{HOSTNAME}/media/lowres_previews/{local_filename}"

                                search_results[platform].append(product_dict)

                    if platform == "landsat8":

                        downloader = l8_downloader.L8Downloader(
                            config_path, verbose=False
                        )

                        results = downloader.search_for_products(
                            "LANDSAT_8_C1", wkt_footprint, arg_list, detailed=True, realtime=False
                        )

                        module_logger.info(len(results))

                        for tile in results:
                            # Steps
                            # Download preview image to media folder
                            # update low res preview url for each tile.
                            module_logger.debug(
                                "trying to download lowres preview url----"
                            )
                            local_filename = download_file(tile["preview_url"])
                            module_logger.debug(HOSTNAME)

                            module_logger.debug(tile)

                            tile["geojson"] = create_geojson_feature(tile)

                            if local_filename:
                                module_logger.debug(
                                    f"http://{HOSTNAME}/media/lowres_previews/{local_filename}"
                                )
                                tile[
                                    "preview_url"
                                ] = f"http://{HOSTNAME}/media/lowres_previews/{local_filename}"

                        search_results[platform] = results

                # Code below is a task for downloading and creating higher resolution previews for each tile (less than 50% cloud)
                # TODO: implement with celery task queue instead of django-workers (unreliable connection to postgres database)
                # for platform in search_results.keys():
                #     for result in search_results[platform]:
                #         print('DJANGO WORKERS TASK')
                #         print(result)
                #         result_serializable = {
                #             'platform_name': result['geojson']['properties']['platform_name'],
                #             'name': result['geojson']['properties']['name'],
                #             'dataset_name': result['geojson']['properties']['dataset_name'],
                #             'entity_id': result['geojson']['properties']['entity_id'],
                #             'api_source': result['geojson']['properties']['api_source'],
                #         }
                #         download_fullrespreview(result_serializable, result_serializable['api_source'])

                if search_results:
                    return Response(
                        {
                            "data": {
                                "id": str(uuid.uuid4()),
                                "uploaded_file_url_list": files_urls,
                                "wkt_footprint": wkt_footprint,
                                "wkt_vis_list": visualization_wkt_list,
                                "mgrs_list": mgrs_list,
                                "wrs_list": wrs_list,
                                "sensor_list": platforms,
                                "wrs_geojson": wrs_geojson,
                                "mgrs_geojson": mgrs_geojson,
                                "tile_results": search_results,
                            }
                        }
                    )
                else:
                    return Response(
                        {
                            "data": {
                                "id": str(uuid.uuid4()),
                                "uploaded_file_url_list": files_urls,
                                "wkt_footprint": wkt_footprint,
                                "wkt_vis_list": visualization_wkt_list,
                                "mgrs_list": mgrs_list,
                                "wrs_list": wrs_list,
                                "tile_results": [],
                            }
                        }
                    )
        else:
            return Response({"error": "Missing required shapefiles data"})


def submit_area_query(request):
    if request.method == "POST" and request.FILES.getlist("shapefiles"):

        files_urls = []

        shapefile_uploaded = None

        random_rename_string = get_random_string(length=8)

        for afile in request.FILES.getlist("shapefiles"):

            module_logger.debug(afile.name)

            fs = FileSystemStorage()

            # check if a file with the same name already exists
            full_path = Path(settings.MEDIA_ROOT, afile.name)
            module_logger.debug(afile.name)
            if full_path.exists():
                filename = (
                    Path(afile.name).stem
                    + random_rename_string
                    + Path(afile.name).suffix
                )
            else:
                filename = afile.name

            filename = fs.save(filename, afile)

            uploaded_file_url = fs.url(filename)

            if Path(filename).suffix == ".shp":
                module_logger.debug(uploaded_file_url)
                module_logger.debug(filename)
                shapefile_uploaded = Path(settings.MEDIA_ROOT, filename)

                module_logger.debug(shapefile_uploaded)

            files_urls.append(uploaded_file_url)

    return

    #     if shapefile_uploaded:
    #         wkt_footprint = grid_intersect.get_wkt_from_shapefile(str(shapefile_uploaded))

    #         mgrs_list = grid_intersect.find_mgrs_intersection(wkt_footprint)
    #         wrs_list = grid_intersect.find_wrs_intersection(wkt_footprint)

    #         # config_path
    #         # landsat_downloader query here
    #         # search_for_products_by_tile
    #         # search_for_products_by_tile(self, dataset_name, tile_list, query_dict, just_entity_ids=False, write_to_csv=False, detailed=False):

    #         date_start = datetime.strptime('20180601', '%Y%m%d')
    #         date_end = datetime.strptime('20180630', '%Y%m%d')

    #         arg_list = {
    #             'date_start': date_start,
    #             'date_end': date_end
    #         }

    #         arg_list['cloud_percent'] = 100

    #         config_path = Path(Path(__file__).absolute().parent.parent, 'landsat_downloader', 'config.json')
    #         print(config_path)
    #         downloader = l8_downloader.L8Downloader(config_path, verbose=False)
    #         search_results = downloader.search_for_products_by_tile('SENTINEL_2A', mgrs_list, arg_list, detailed=True)

    #         print(search_results)

    #         return render(request, 's2d2_app/multi_file_upload.html', {
    #                 'uploaded_file_url_list': files_urls,
    #                 'wkt_footprint': wkt_footprint,
    #                 'mgrs_list': mgrs_list,
    #                 'wrs_list': wrs_list
    #             })

    #     else:
    #         return render(request, 's2d2_app/multi_file_upload.html', {
    #                 'uploaded_file_url_list': files_urls
    #             })
    # return render(request, 's2d2_app/multi_file_upload.html')


from django.middleware.csrf import get_token


class CSRFGeneratorView(APIView):
    def get(self, request):
        csrf_token = get_token(request)
        return Response(csrf_token)


class Version(APIView):
    def get(self, request):
        data = {"version": settings.API_VERSION}
        return Response(data)
