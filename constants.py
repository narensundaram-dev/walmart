BASE_URL = "https://grocery.walmart.com"
HEADERS = {"Content-Type": "application/json"}
PRODUCT_DETAILS = ["brand", "productType", "shortDescription", "description", "ingredients", "storageType"]


STORE_ID = 915
LIMIT = 50


URL_PRODUCT_LIST = BASE_URL + "/v4/api/products/browse?count={limit}&page={page}&storeId={store_id}&taxonomyNodeId={node_id}"
URL_PRODUCT_DETAIL = BASE_URL + "/v3/api/products/{sku}?itemFields=all&storeId={store_id}"


INFO, DEBUG = "INFO", "DEBUG"
