import os
import re
import math
import json
import shutil
import logging
import argparse
import traceback
from datetime import datetime as dt
from urllib.parse import urlsplit, parse_qs

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

    def __init__(self, product_id, url, manager):
        self.product_id = product_id
        self.url = url
        self.manager = manager

    def save_image(self, url_image):
        if url_image != "NA":
            extn = re.search(r"(\.\w+)\?", url_image).group(1)
            fp = os.path.join(self.manager.dir_images, f"{self.product_id}.{extn}")
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
        url = URL_PRODUCT_DETAIL.format(product_id=self.product_id, store_id=STORE_ID)
        response = requests.get(url, headers=HEADERS).json()

        details = self.get_details(response.get("detailed", {}))
        url_image = response.get("basic", {}).get("image", {}).get("large", "NA")
        self.save_image(url_image)
        return {
            "sku": response.get("sku", "NA"),
            "product_code": response.get("detailed", {}).get("productCode", "NA"),
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

    def __init__(self, url, type_, id_, manager):
        self.url = url
        self.type = type_
        self.id = id_
        self.manager = manager

    def get(self):
        page = 1
        data = []

        count = 1
        while True:
            if self.type == "node":
                url = URL_PRODUCT_LIST_NODE.format(limit=LIMIT, page=page, store_id=STORE_ID, node_id=self.id)
            else:
                url = URL_PRODUCT_LIST_SHELF.format(limit=LIMIT, page=page, store_id=STORE_ID, shelf_id=self.id)
            response = requests.get(url=url, headers=HEADERS).json()

            if not response["products"]:
                break

            log.info(f"            *** Page: {page} ***            ")
            for product in response["products"]:
                product_detail = WalmartProductDetail(product["USItemId"], product["basic"]["productUrl"], self.manager)
                data.append(product_detail.get())

                if count % 5 == 0:
                    log.info(f"{count} products has been fetched so far ...")
                count += 1

            page += 1

        return data


class WalmartManager:
    dir_output = os.path.join(os.getcwd(), "output")
    dir_images = os.path.join(dir_output, "images")

    def __init__(self, url):
        self.url = url
        self.type = "node" if "aisle=" in self.url else "shelf"
        self.meta = self.load_meta_data()

        self.dir_xlsx = os.path.join(WalmartManager.dir_output, "xlsx")
        if self.type == "node":
            self.dir_images = os.path.join(WalmartManager.dir_output, "images", self.meta["categ"])
        else:
            self.dir_images = os.path.join(WalmartManager.dir_output, "images", f"shelf_id_{self.get_shelf_id()}")
        self.file_xlsx = os.path.join(self.dir_xlsx, self.get_xlsx_filename())

        self.data = []

    def setup(self):
        os.makedirs(self.dir_xlsx, exist_ok=True)
        os.makedirs(self.dir_images, exist_ok=True)

    def load_meta_data(self):
        meta = {}

        if self.type == "node":
            node_id = self.get_node_id()
            url = URL_PRODUCT_LIST_NODE.format(limit=1, page=1, store_id=STORE_ID, node_id=node_id)
        else:
            shelf_id = self.get_shelf_id()
            url = URL_PRODUCT_LIST_NODE.format(limit=1, page=1, store_id=STORE_ID, node_id=shelf_id)

        response = requests.get(url, headers=HEADERS).json()

        meta["categ"], meta["sub_categ"] = self.get_categ(response)
        return meta

    def get_xlsx_filename(self):
        if self.type == "node":
            categ, sub_categ = self.meta["categ"], self.meta["sub_categ"]
            return f"{categ}_{sub_categ}.xlsx"
        else:
            return f"shelf_id_{self.get_shelf_id()}.xlsx"

    def get_categ(self, response):
        categs = []
        for categ in response.get("browseTitles", []):
            categs.append(categ["name"].replace(" ", "-"))

        if categs:
            return categs[:2] if len(categs) >= 2 else [categs[0], ""]
        else:
            return "", ""

    def get_node_id(self):
        params = parse_qs(urlsplit(self.url).query)
        return params["aisle"][0]

    def get_shelf_id(self):
        params = parse_qs(urlsplit(self.url).query)
        return params["shelfId"][0]

    def get(self):
        self.data = WalmartProductList(
            url=self.url, 
            type_=self.type,
            id_=self.get_node_id() if self.type == "node" else self.get_shelf_id(),
            manager=self
            ).get()
        return self

    def save(self):
        df = pd.DataFrame(self.data)
        df["category"] = self.meta["categ"].replace("-", " ")
        df["sub_category"] = self.meta["sub_categ"].replace("-", " ")
        columns = ["sku", "name", "category", "sub_category", "mrp", "cost_price", "weight", "details", "nutrition_facts", "url_image", "url"]
        df_ordered = df[columns]
        df_ordered.to_excel(self.file_xlsx, index=False)
        log.info(f"Fetched data ({len(df_ordered)}) has been stored in {self.file_xlsx} file")


def main():
    start = dt.now()
    log.info("Script starts at: {}".format(start.strftime("%d-%m-%Y %H:%M:%S %p")))

    with open("url_categories.txt", "r") as f:
        urls = f.read().strip().split("\n")

    urls_done = []
    try:
        for url in urls:
            print("-----------" + "-" * len(url))
            print(f"Fetching: {url}")
            print("-----------" + "-" * len(url))
            walmart = WalmartManager(url)
            walmart.setup()
            walmart.get().save()

            urls_done.append(url)
    except Exception as e:
        log.error(f"Error: {e}")
        print(traceback.print_exc())
        urls_pending = list((set(urls) - set(urls_done)))
        with open("urls_pending.txt", "w+") as f:
            f.write("\n".join(urls_pending))
            log.info("Pending urls to be fetched are stored in urls_pending.txt")
    
    end = dt.now()
    log.info("Script ends at: {}".format(end.strftime("%d-%m-%Y %H:%M:%S %p")))
    elapsed = round(((end - start).seconds / 60), 4)
    log.info("Time Elapsed: {} minutes".format(elapsed))


if __name__ == "__main__":
    main()
