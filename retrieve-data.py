#!/usr/bin/env python3

# RetrieveData - Download seal images given their metadata files
# Copyright (C) 2021
# Joana Bergsiek, Leonard Geier, Lisa Ihde,
# Tobias Markus, Dominik Meier, Paul Methfessel
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

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

        image_url = root.find(
            "lido:administrativeMetadata/lido:resourceWrap/lido:resourceSet/"
            "lido:resourceRepresentation/lido:linkResource",
            ns,
        ).text

        response = requests.get(image_url, stream=True)
        image_filename = (
            metadata_file.split(".")[0] + "-img." + image_url.split(".")[-1]
        )
        with open(path.join(args.output_dir, image_filename), "wb") as image_file:
            for data in response.iter_content():
                image_file.write(data)

    except Exception as e:
        error_message = "Failed at metadata file " + metadata_file
        if args.no_exception:
            print(error_message)
            return
        else:
            raise Exception("Failed at metadata file " + metadata_file) from e


if __name__ == "__main__":
    parser = ArgumentParser()

    parser.add_argument(
        "metadata_dir",
        help="Directory containing the metadata XML files for the dataset",
    )
    parser.add_argument("output_dir", help="Destination directory for the data")
    parser.add_argument(
        "-j", "--jobs", type=int, default=1, help="Number of simultaneous jobs"
    )
    parser.add_argument(
        "--no_exception",
        default=False,
        action="store_true",
        help="If a failed file should through an exception or not",
    )

    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    metadata_files = os.listdir(args.metadata_dir)
    retrieve_image = partial(_retrieve_image, args)
    with Pool(args.jobs) as p:
        list(
            tqdm(
                p.imap(retrieve_image, metadata_files),
                total=len(metadata_files),
                desc="Retrieving images",
            )
        )
