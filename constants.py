BASE_URL = "https://grocery.walmart.com"
HEADERS = {"Content-Type": "application/json"}
PRODUCT_DETAILS = ["brand", "productType", "shortDescription", "description", "ingredients", "storageType"]


STORE_ID = 915
LIMIT = 50

RETRY_LIMIT = 10
URL_PRODUCT_LIST_NODE = BASE_URL + "/v4/api/products/browse?count={limit}&page={page}&storeId={store_id}&taxonomyNodeId={node_id}"
URL_PRODUCT_LIST_SHELF = BASE_URL + "/v4/api/products/browse?count={limit}&page={page}&storeId={store_id}&shelfId={shelf_id}"
URL_PRODUCT_DETAIL = BASE_URL + "/v3/api/products/{product_id}?itemFields=all&storeId={store_id}"
