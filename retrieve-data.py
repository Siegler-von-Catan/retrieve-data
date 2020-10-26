#!/usr/bin/env python3

from argparse import ArgumentParser
from tqdm import tqdm
from multiprocessing import Pool
from functools import partial
import os
import os.path as path
import xml.etree.ElementTree as ET
import requests

ns = {"lido": "http://www.lido-schema.org"}

def _retrieve_image(args, metadata_file):
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
    

if __name__ == "__main__":
    parser = ArgumentParser()

    parser.add_argument("metadata_dir", help="Directory containing the metadata XML files for the dataset")
    parser.add_argument("output_dir", help="Destination directory for the data")
    parser.add_argument("-j", "--jobs", type=int, default=1, help="Number of simultaneous jobs")

    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    metadata_files = os.listdir(args.metadata_dir)
    retrieve_image = partial(_retrieve_image, args)
    with Pool(args.jobs) as p:
        list(tqdm(p.imap(retrieve_image, metadata_files), total=len(metadata_files), desc="Retrieving images"))
