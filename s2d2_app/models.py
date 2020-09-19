from django.db import models

# Create your models here.

class Shapefile(models.Model):
    description = models.CharField(max_length=255)
    # Required
    shp_shp = models.FileField(upload_to='shapefiles/')
    shp_dbf = models.FileField(upload_to='shapefiles/')
    shp_shx = models.FileField(upload_to='shapefiles/')
    shp_prj = models.FileField(upload_to='shapefiles/')
    # Optional
    shp_xml = models.FileField(upload_to='shapefiles/', blank=True)
    shp_sbx = models.FileField(upload_to='shapefiles/', blank=True)

    uploaded_at = models.DateTimeField(auto_now_add=True)


"""
"E:\Data\Shapefiles\AB_Agr_Mask_WGS84.sbn"
"E:\Data\Shapefiles\AB_Agr_Mask_WGS84.sbx"
"E:\Data\Shapefiles\AB_Agr_Mask_WGS84.shp"
"E:\Data\Shapefiles\AB_Agr_Mask_WGS84.shx"
"E:\Data\Shapefiles\AB_Agr_Mask_WGS84.dbf"
"E:\Data\Shapefiles\AB_Agr_Mask_WGS84.prj"
"""