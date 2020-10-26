#!/usr/bin/env python3

from argparse import ArgumentParser
from tqdm import tqdm
import os
import os.path as path
import xml.etree.ElementTree as ET
import requests

parser = ArgumentParser()

parser.add_argument("metadata_dir", help="Directory containing the metadata XML files for the dataset")
parser.add_argument("output_dir", help="Destination directory for the data")

args = parser.parse_args()

os.makedirs(args.output_dir, exist_ok=True)

ns = {"lido": "http://www.lido-schema.org"}

for metadata_file in tqdm(os.listdir(args.metadata_dir), desc="Retrieving images"):
    try:
        tree = ET.parse(path.join(args.metadata_dir, metadata_file))
        root = tree.getroot()
        image_url = root.find("lido:administrativeMetadata/lido:resourceWrap/lido:resourceSet/lido:resourceRepresentation/lido:linkResource", ns).text
        
        response = requests.get(image_url, stream=True)
        image_filename = metadata_file.split(".")[0] + "-img." + image_url.split(".")[-1]
        with open(path.join(args.output_dir, image_filename), "wb") as image_file:
            for data in response.iter_content():
                image_file.write(data)

    except Exception as e:
        raise Exception("Failed at metadata file " + metadata_file) from e
    
