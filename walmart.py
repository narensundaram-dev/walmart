import os
import re
import sys
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
            extn = re.search(r"(\.[\w-]+)\?", url_image).group(1)
            fp = os.path.join(self.manager.dir_images, f"{self.product_id}.{extn}")

            retry_limit, retry = RETRY_LIMIT, 0
            while True:
                try:
                    if retry >= retry_limit:
                        log.error("Retry limit exceeded. Exit!")
                        sys.exit(1)

                    response = requests.get(url_image, stream=True)
                    if 200 <= response.status_code <= 299:
                        with open(fp, "wb+") as f:
                            response.raw.decode_content = True
                            shutil.copyfileobj(response.raw, f) 
                        break
                    else:
                        retry += 1
                        log.error(f"Failed to download the product image. Got {response.status_code}. Retrying ({retry}) ...")
                except Exception as e:
                    retry += 1
                    log.error(f"Failed to fetch the product image. No response. Retrying ({retry}) ...")


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

        retry_limit, retry = RETRY_LIMIT, 0
        while True:
            try:
                if retry >= retry_limit:
                    log.error("Retry limit exceeded. Exit!")
                    sys.exit(1)

                response = requests.get(url, headers=HEADERS)
                if 200 <= response.status_code <= 299:
                    data = response.json()
                    details = self.get_details(data.get("detailed", {}))
                    url_image = data.get("basic", {}).get("image", {}).get("large", "NA")
                    self.save_image(url_image)
                    return {
                        "sku": data.get("sku", "NA"),
                        "product_code": data.get("detailed", {}).get("productCode", "NA"),
                        "name": data.get("basic", {}).get("name", "NA"),
                        "mrp": data.get("store", {}).get("price", {}).get("previousPrice", None),
                        "cost_price": data.get("store", {}).get("price", {}).get("displayPrice", None),
                        "weight": data.get("store", {}).get("price", {}).get("displayUnitPrice", "NA"),
                        "url_image": url_image,
                        "details": details,
                        "nutrition_facts": json.dumps(data.get("nutritionFacts", "NA")),
                        "url_product": BASE_URL + self.url
                    }
                else:
                    retry += 1
                    log.error(f"Failed to fetch the product. Got {response.status_code}. Retrying ({retry}) ...")

            except Exception as e:
                    retry += 1
                    log.error(f"Failed to fetch the product from {url}. No response. Retrying ({retry}) ...")


class WalmartProductList:

    def __init__(self, url, type_, id_, manager, args):
        self.url = url
        self.type = type_
        self.id = id_
        self.manager = manager
        self.args = args
        self.page_ranges = None

    def get_page_ranges(self):
        if self.args.batch:
            print("Enter page ranges:")
            print("page_from: ")
            page_from = int(input())
            print("page_to: ")
            page_to = int(input())
            page_total = self.manager.meta["total_pages"]

            if not ((1 <= page_from <= page_total) or (1 <= page_from <= page_total)):
                log.error(f"Page ranges should be within 1...{page_total}!")
                self.get_page_ranges()

            self.page_ranges = [page_from, page_to]
            return page_from, page_to
        else:
            return 1, self.manager.meta["total_pages"]

    def get(self):
        page, page2 = self.get_page_ranges()
        count, data, error = 1, [], False
        try:
            while True:
                if self.type == "node":
                    url = URL_PRODUCT_LIST_NODE.format(limit=LIMIT, page=page, store_id=STORE_ID, node_id=self.id)
                else:
                    url = URL_PRODUCT_LIST_SHELF.format(limit=LIMIT, page=page, store_id=STORE_ID, shelf_id=self.id)
                response = requests.get(url=url, headers=HEADERS).json()

                log.info(f"            *** Page: {page} ***            ")
                for product in response["products"]:
                    product_detail = WalmartProductDetail(product["USItemId"], product["basic"]["productUrl"], self.manager).get()
                    data.append(product_detail)

                    if count % 10 == 0:
                        log.info(f"{count} products has been fetched so far ...")
                    count += 1

                page += 1
                if page > page2:
                    break

        except Exception as e:
            error = True
            log.error(f"Error: {e}")
            traceback.print_exc()
        finally:
            return data, error


class WalmartManager:
    dir_output = os.path.join(os.getcwd(), "output")
    dir_images = os.path.join(dir_output, "images")

    def __init__(self, url, args):
        self.url = url
        self.args = args
        self.type = "node" if "aisle=" in self.url else "shelf"
        self.meta = self.load_meta_data()

        self.dir_xlsx = os.path.join(WalmartManager.dir_output, "xlsx")
        if self.type == "node":
            self.dir_images = os.path.join(WalmartManager.dir_output, "images", self.meta["categ"])
        else:
            self.dir_images = os.path.join(WalmartManager.dir_output, "images", f"{self.meta['shelf_name']}")

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
            url = URL_PRODUCT_LIST_SHELF.format(limit=1, page=1, store_id=STORE_ID, shelf_id=shelf_id)

        response = requests.get(url, headers=HEADERS).json()
        meta["categ"], meta["sub_categ"] = self.get_categ(response)
        meta["shelf_name"] = response.get("manualShelfName", "NA").replace(" ", "-")
        meta["total_products"] = response.get("totalCount", 0)
        meta["total_pages"] = math.ceil(meta["total_products"] / LIMIT)
        log.info(f"{meta['total_products']} products found! {meta['total_pages']} pages to be scraped.")
        return meta

    def get_xlsx_filepath(self, filename_suffix=""):
        if self.type == "node":
            categ, sub_categ = self.meta["categ"], self.meta["sub_categ"]
            filename = f"{categ}_{sub_categ}"
        else:
            filename = f"{self.meta['shelf_name']}"
        filename += filename_suffix
        return os.path.join(self.dir_xlsx, f"{filename}.xlsx")

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
        product_list = WalmartProductList(
            url=self.url, 
            type_=self.type,
            id_=self.get_node_id() if self.type == "node" else self.get_shelf_id(),
            manager=self,
            args=self.args
        )
        data, error = product_list.get()
        self.data = data

        suffix = f"_{product_list.page_ranges[0]}_{product_list.page_ranges[1]}" if self.args.batch else ""
        xlsx_file = self.get_xlsx_filepath(suffix)
        self.save(fp=xlsx_file)
        if error:
            sys.exit(1)

        return self.data

    def save(self, fp):
        if self.data:
            df = pd.DataFrame(self.data)
            df["category"] = self.meta["categ"].replace("-", " ")
            df["sub_category"] = self.meta["sub_categ"].replace("-", " ")
            df["url_category"] = self.url
            columns = [
                "sku", "name", "category", "sub_category", "mrp", "cost_price", 
                "weight", "details", "nutrition_facts", "url_image", "url_product", "url_category"]
            df_ordered = df[columns]
            df_ordered.to_excel(fp, index=False)
            log.info(f"Fetched data ({len(df_ordered)}/{self.meta['total_products']}) has been stored in {fp}.")
        else:
            log.info("Nothing to save!")


def get_args():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--batch-mode', dest="batch", action="store_true")
    return arg_parser.parse_args()


def main():
    start = dt.now()
    log.info("Script starts at: {}".format(start.strftime("%d-%m-%Y %H:%M:%S %p")))

    args = get_args()
    with open("url_categories.txt", "r") as f:
        urls = f.read().strip().split("\n")
        if args.batch:
            if len(urls) > 1:
                log.error("Only one url can be loaded in url_categories.txt in Batch Mode!")
                sys.exit(1)

    urls_done = []
    try:
        for url in urls:
            print("-----------" + "-" * len(url))
            print(f"Fetching: {url}")
            print("-----------" + "-" * len(url))
            walmart = WalmartManager(url, args)
            walmart.setup()
            walmart.get()

            urls_done.append(url)
    except Exception as e:
        log.error(f"Error: {e}")
        print("Traceback:\n", traceback.print_exc())

        urls_pending = list((set(urls) - set(urls_done)))
        with open("urls_pending.txt", "w+") as f:
            f.write("\n".join(urls_pending))
            log.info("Pending urls to be fetched are stored in urls_pending.txt")
        sys.exit(1)

    end = dt.now()
    log.info("Script ends at: {}".format(end.strftime("%d-%m-%Y %H:%M:%S %p")))
    elapsed = round(((end - start).seconds / 60), 4)
    log.info("Time Elapsed: {} minutes".format(elapsed))


if __name__ == "__main__":
    main()
