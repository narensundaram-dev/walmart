import os
import re
import math
import json
import shutil
import logging
import argparse
import traceback
from pprint import pprint
from datetime import datetime as dt
from urllib.parse import urlsplit, parse_qs
from concurrent.futures import as_completed, ThreadPoolExecutor

import requests
import pandas as pd

from constants import *


def get_logger(file_, log_level=logging.INFO):
    log = logging.getLogger(file_.split('/')[-1])
    log_level = logging.INFO
    log.setLevel(log_level)
    log_handler = logging.StreamHandler()
    log_formatter = logging.Formatter('%(levelname)s: %(asctime)s - %(name)s:%(lineno)d - %(message)s')
    log_handler.setFormatter(log_formatter)
    log.addHandler(log_handler)
    return log


log = get_logger(__file__)


class WalmartProductDetail:

    def __init__(self, sku, url):
        self.sku = sku
        self.url = url

    def save_image(self, url_image):
        if url_image != "NA":
            fp = os.path.join(WalmartManager.dir_images, f"{self.sku}.jpeg")
            response = requests.get(url_image, stream=True)
            if response.status_code == 200:
                with open(fp, "wb+") as f:
                    response.raw.decode_content = True
                    shutil.copyfileobj(response.raw, f) 

    def humanize_title(self, title):
        title_arr = map(lambda x: x.title(), re.findall(r'[a-zA-Z](?:[a-z]+|[A-Z]*(?=[A-Z]|$))', title))
        return " ".join(title_arr)

    def remove_tags(self, html):
        html = re.sub(r"<li>", "\n", html)
        text = re.sub(r"</?\w+\s*/?>", "", html)
        return text

    def get_details(self, dict_details):
        details = ""
        for key, value in dict_details.items():
            if key in PRODUCT_DETAILS:
                key = self.humanize_title(key)
                value = self.remove_tags(value).replace("\n", "\n\t")
                details += f"{key}:\n\t{value}\n\n"
        return details

    def get(self):
        url = URL_PRODUCT_DETAIL.format(sku=self.sku, store_id=STORE_ID)
        response = requests.get(url, headers=HEADERS).json()

        details = self.get_details(response.get("detailed", {}))
        url_image = response.get("basic", {}).get("image", {}).get("large", "NA")
        self.save_image(url_image)
        return {
            "sku": self.sku,
            "name": response.get("basic", {}).get("name", "NA"),
            "mrp": response.get("store", {}).get("price", {}).get("previousPrice", None),
            "cost_price": response.get("store", {}).get("price", {}).get("displayPrice", None),
            "weight": response.get("store", {}).get("price", {}).get("displayUnitPrice", "NA"),
            "url_image": url_image,
            "details": details,
            "nutrition_facts": json.dumps(response.get("nutritionFacts", "NA")),
            "url": BASE_URL + self.url
        }


class WalmartProductList:

    def __init__(self, url, page_count, node_id):
        self.url = url
        self.page_count = page_count
        self.node_id = node_id

    def get(self):
        page = 1
        data = []

        count = 1
        while True:
            log.info(f"            *** Page: {page} ***            ")
            url = URL_PRODUCT_LIST.format(limit=LIMIT, page=page, store_id=STORE_ID, node_id=self.node_id)
            response = requests.get(url=url, headers=HEADERS).json()

            if not response["products"]:
                break

            for idx, product in enumerate(response["products"]):
                product_detail = WalmartProductDetail(product["USItemId"], product["basic"]["productUrl"])
                data.append(product_detail.get())
                log.info(f"{count} products has been fetched so far ...")
                count += 1

            page += 1

        return data


class WalmartManager:
    dir_output = "output"
    dir_xlsx = os.path.join(dir_output, "xlsx")
    dir_images = os.path.join(dir_output, "images")

    def __init__(self, url):
        self.url = url
        self.meta = self.load_meta_data()

        self.dir_xlsx = WalmartManager.dir_xlsx
        self.dir_images = os.path.join(WalmartManager.dir_images, self.meta["categ"])
        WalmartManager.dir_images = self.dir_images
        self.file_xlsx = os.path.join(self.dir_xlsx, self.get_xlsx_filename())

        self.data = []

    def setup(self):
        os.makedirs(self.dir_xlsx, exist_ok=True)
        os.makedirs(self.dir_images, exist_ok=True)

    def load_meta_data(self):
        meta = {}

        node_id = self.get_node_id()
        url = URL_PRODUCT_LIST.format(limit=1, page=1, store_id=STORE_ID, node_id=node_id)
        response = requests.get(url, headers=HEADERS).json()

        meta["categ"], meta["sub_categ"] = self.get_categ(response)
        meta["page_count"] = self.get_page_count(response)
        return meta

    def get_xlsx_filename(self):
        categ, sub_categ = self.meta["categ"], self.meta["sub_categ"]
        return f"{categ}_{sub_categ}.xlsx"

    def get_categ(self, response):
        categs = []
        for categ in response["browseTitles"]:
            categs.append(categ["name"].replace(" ", "-"))

        return categs[:2] if len(categs) >= 2 else [categs[0], ""]

    def get_page_count(self, response):
        total = response["totalCount"]
        log.info(f"{total} products found!")
        return math.ceil(total / LIMIT)

    def get_node_id(self):
        params = parse_qs(urlsplit(self.url).query)
        return params["aisle"][0]

    def get(self):
        self.data = WalmartProductList(url=self.url, page_count=self.meta["page_count"], node_id=self.get_node_id()).get()
        return self

    def save(self):
        df = pd.DataFrame(self.data)
        df.to_excel(self.file_xlsx, index=False)
        log.info("Fetched data has been stored in {} file".format(self.file_xlsx))


def get_args():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-log-level', '--log_level', type=str, choices=(INFO, DEBUG), default=INFO)
    return arg_parser.parse_args()


def main():
    start = dt.now()
    log.info("Script starts at: {}".format(start.strftime("%d-%m-%Y %H:%M:%S %p")))

    args = get_args()
    with open("url_categories.txt", "r") as f:
        for url in f.readlines():
            url = url.strip()

            print("-----------" + "-" * len(url))
            print(f"Fetching: {url}")
            print("-----------" + "-" * len(url))
            walmart = WalmartManager(url)
            walmart.setup()
            walmart.get().save()
    
    end = dt.now()
    log.info("Script ends at: {}".format(end.strftime("%d-%m-%Y %H:%M:%S %p")))
    elapsed = round(((end - start).seconds / 60), 4)
    log.info("Time Elapsed: {} minutes".format(elapsed))


if __name__ == "__main__":
    main()
