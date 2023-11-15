# Emulates GDAL's gdal_polygonize.py

import argparse
import logging
import subprocess
import sys

import fiona
import numpy as np
import rasterio
from rasterio.features import shapes


logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logger = logging.getLogger('rasterio_polygonize')


def main(raster_file, vector_file, driver, mask_value):
    
    with rasterio.drivers():
        
        with rasterio.open(raster_file) as src:
            image = src.read(1)
        
        if mask_value is not None:
            mask = image == mask_value
        else:
            mask = None
        
        results = (
            {'properties': {'raster_val': v}, 'geometry': s}
            for i, (s, v) 
            in enumerate(
                shapes(image, mask=mask, transform=src.affine)))

        with fiona.open(
                vector_file, 'w', 
                driver=driver,
                crs=src.crs,
                schema={'properties': [('raster_val', 'int')],
                        'geometry': 'Polygon'}) as dst:
            dst.writerecords(results)
    
    return dst.name