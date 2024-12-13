import requests
import pandas

# https://open.toronto.ca/dataset/neighbourhood-crime-rates/

# Toronto Open Data is stored in a CKAN instance. It's APIs are documented here:
# https://docs.ckan.org/en/latest/api/

# To hit our API, you'll be making requests to:
base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"

# Datasets are called "packages". Each package can contain many "resources"
# To retrieve the metadata for this package and its resources, use the package name in this page's URL:
url = base_url + "/api/3/action/package_show"
params = {"id": "neighbourhood-crime-rates"}
package = requests.get(url, params=params).json()

# To get resource data:
for idx, resource in enumerate(package["result"]["resources"]):

    # for datastore_active resources:
    if resource["datastore_active"]:
        # To get all records in CSV format:
        url = base_url + "/datastore/dump/" + resource["id"]
        resource_dump_data = requests.get(url).content

        with open("demo_dump.csv", "wb") as f:
            f.write(resource_dump_data)

        # print(resource_dump_data)

        # To selectively pull records and attribute-level metadata:
        url = base_url + "/api/3/action/datastore_search"
        p = {"id": resource["id"], "limit": 5000}  # adjustable params
        resource_search_data = requests.get(url, params=p).json()["result"]["records"]

        df = pandas.DataFrame(resource_search_data)
        csv_data = df.to_csv("demo_datastore.csv", index=False)

        # print(resource_search_data)
        # This API call has many parameters. They're documented here:
        # https://docs.ckan.org/en/latest/maintaining/datastore.html

    # To get metadata for non datastore_active resources:
    if not resource["datastore_active"]:
        url = base_url + "/api/3/action/resource_show?id=" + resource["id"]
        resource_metadata = requests.get(url).json()
        print(resource_metadata)
        # From here, you can use the "url" attribute to download this file


print("Search a DataStore resource.\n\n    The datastore_search action allows you to search data in a resource. By\n  "
      "  default 100 rows are returned - see the `limit` parameter for more info.\n\n    A DataStore resource that "
      "belongs to a private CKAN resource can only be\n    read by you if you have access to the CKAN resource and "
      "send the\n    appropriate authorization.\n\n    :param resource_id: id or alias of the resource to be searched "
      "against\n    :type resource_id: string\n    :param filters: matching conditions to select, e.g\n               "
      "     {\"key1\": \"a\", \"key2\": \"b\"} (optional)\n    :type filters: dictionary\n    :param q: full text "
      "query. If it's a string, it'll search on all fields on\n              each row. If it's a dictionary as {"
      "\"key1\": \"a\", \"key2\": \"b\"},\n              it'll search on each specific field (optional)\n    :type q: "
      "string or dictionary\n    :param distinct: return only distinct rows (optional, default: false)\n    :type "
      "distinct: bool\n    :param plain: treat as plain text query (optional, default: true)\n    :type plain: bool\n "
      "   :param language: language of the full text query\n                     (optional, default: english)\n    "
      ":type language: string\n    :param limit: maximum number of rows to return\n        (optional, "
      "default: ``100``, unless set in the site's configuration\n        ``ckan.datastore.search.rows_default``, "
      "upper limit: ``32000`` unless\n        set in site's configuration ``ckan.datastore.search.rows_max``)\n    "
      ":type limit: int\n    :param offset: offset this number of rows (optional)\n    :type offset: int\n    :param "
      "fields: fields to return\n                   (optional, default: all fields in original order)\n    :type "
      "fields: list or comma separated string\n    :param sort: comma separated field names with ordering\n           "
      "      e.g.: \"fieldname1, fieldname2 desc\"\n    :type sort: string\n    :param include_total: True to return "
      "total matching record count\n                          (optional, default: true)\n    :type include_total: "
      "bool\n    :param total_estimation_threshold: If \"include_total\" is True and\n        "
      "\"total_estimation_threshold\" is not None and the estimated total\n        (matching record count) is above "
      "the \"total_estimation_threshold\" then\n        this datastore_search will return an *estimate* of the total, "
      "rather\n        than a precise one. This is often good enough, and saves\n        computationally expensive "
      "row counting for larger results (e.g. \u003E100000\n        rows). The estimated total comes from the "
      "PostgreSQL table statistics,\n        generated when Express Loader or DataPusher finishes a load, or by\n     "
      "   autovacuum. NB Currently estimation can't be done if the user specifies\n        'filters' or 'distinct' "
      "options. (optional, default: None)\n    :type total_estimation_threshold: int or None\n    :param "
      "records_format: the format for the records return value:\n        'objects' (default) list of {fieldname1: "
      "value1, ...} dicts,\n        'lists' list of [value1, value2, ...] lists,\n        'csv' string containing "
      "comma-separated values with no header,\n        'tsv' string containing tab-separated values with no header\n  "
      "  :type records_format: controlled list\n\n\n    Setting the ``plain`` flag to false enables the entire "
      "PostgreSQL\n    `full text search query language`_.\n\n    A listing of all available resources can be found "
      "at the\n    alias ``_table_metadata``.\n\n    .. _full text search query language: "
      "http://www.postgresql.org/docs/9.1/static/datatype-textsearch.html#DATATYPE-TSQUERY\n\n    If you need to "
      "download the full resource, read :ref:`dump`.\n\n    **Results:**\n\n    The result of this action is a "
      "dictionary with the following keys:\n\n    :rtype: A dictionary with the following keys\n    :param fields: "
      "fields/columns and their extra metadata\n    :type fields: list of dictionaries\n    :param offset: query "
      "offset value\n    :type offset: int\n    :param limit: queried limit value (if the requested ``limit`` was "
      "above the\n        ``ckan.datastore.search.rows_max`` value then this response ``limit``\n        will be set "
      "to the value of ``ckan.datastore.search.rows_max``)\n    :type limit: int\n    :param filters: query filters\n "
      "   :type filters: list of dictionaries\n    :param total: number of total matching records\n    :type total: "
      "int\n    :param total_was_estimated: whether or not the total was estimated\n    :type total_was_estimated: "
      "bool\n    :param records: list of matching results\n    :type records: depends on records_format value "
      "passed\n\n    ")