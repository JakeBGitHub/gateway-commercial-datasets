import os
import urllib
import pymongo
import pandas as pd
import numpy as np
from collections import MutableMapping
from decouple import config

mongodb_user = config("mongodb_user", default="")
mongodb_password = config("mongodb_password", default="")


DATABASE = os.getenv("DATABASE", "production")
DATABASE_USER = os.getenv("DATABASE_USER", mongodb_user)
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD", mongodb_password)
DATABASE_URL = "mongodb+srv://{username}:{password}@productioncluster.dbcdg.gcp.mongodb.net/{dbname}?retryWrites=true&w=majority".format(
    username=urllib.parse.quote_plus(DATABASE_USER),
    password=urllib.parse.quote_plus(DATABASE_PASSWORD),
    dbname=DATABASE,
)


def client_connect(url, database):
    client = pymongo.MongoClient(url)
    db = client[database]
    return db


def get_collection(collection, db):
    collect = db[collection]
    return collect


def get_aggregation(collection, query):
    agg = collection.aggregate(query)

    collect_filtered_list = []
    for item in agg:
        collect_filtered_list.append(item)

    return collect_filtered_list


def convert_flatten(dictionary, parent_key="", sep="_"):
    items = []
    for k, v in dictionary.items():
        new_key = parent_key + sep + k if parent_key else k

        if isinstance(v, MutableMapping):
            items.extend(convert_flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def flatten_collection_list(collection):
    flattened_collection_list = []
    for item in collection:
        flattened_item = convert_flatten(item)
        flattened_collection_list.append(flattened_item)
    return flattened_collection_list


def filter_df(df):
    df = df.drop(
        [
            "index",
            "datasetfields_datautility_allowable_uses",
            "datasetv2_accessibility_usage_dataUseLimitation",
            "datasetv2_accessibility_usage_dataUseRequirements",
        ],
        axis=1,
    )

    df = df.rename(
        columns={
            "datasetfields_datautility_title": "Dataset Name",
            "datasetfields_metadataschema_url": "URL",
            "datasetv2_summary_publisher_name": "Publisher",
            "datasetv2_summary_publisher_memberOf": "Member Of",
        }
    )

    df = df[["_id", "pid", "Member Of", "Publisher", "Dataset Name", "URL"]]

    return df


def get_commercial_datasets(tools_df, export_path):

    """
    Definition of Commercial Dataset on the Gateway:

    - If a dataset has an allowable_use score of either Gold || Platinum we can set the dataset to be available for commerical use. Carries the heaviest weight

    - If the dataUtility allowable_use score isn’t available we are only checking the field data_use_limitation for “COMMERCIAL RESEARCH USE || NO RESTRICTIONS“.
    Otherwise commerical use will be set to false.
    """

    commercial_datasets_df = tools_df.iloc[:0]

    commercial_rows = []
    commercial_count = 0
    for index, row in tools_df.iterrows():
        if (
            tools_df["datasetfields_datautility_allowable_uses"][index].strip()
            == "Gold"
            or tools_df["datasetfields_datautility_allowable_uses"][index].strip()
            == "Platinum"
        ):
            commercial_rows.append(row.values)
            commercial_count += 1
        elif tools_df["datasetfields_datautility_allowable_uses"][index] == "" and (
            "NO RESTRICTION"
            in str(tools_df["datasetv2_accessibility_usage_dataUseLimitation"][index])
            or "“COMMERCIAL RESEARCH USE"
            in str(tools_df["datasetv2_accessibility_usage_dataUseLimitation"][index])
        ):
            commercial_rows.append(row.values)
            commercial_count += 1

    commercial_datasets_df = commercial_datasets_df.append(
        pd.DataFrame(commercial_rows, columns=commercial_datasets_df.columns)
    ).reset_index()

    commercial_datasets_df = filter_df(commercial_datasets_df)

    commercial_datasets_df.to_csv(export_path, index=False)


def not_for_profit_datasets(tools_df, export_path):

    """
    List of Not-for-Profit datasets that explicitly say dataUseLimitation == “NOT FOR PROFIT USE” or dataUseRequirement == “NOT FOR PROFIT USE”
    """

    nfp_datasets_df = tools_df.iloc[:0]

    nfp_rows = []
    nfp_count = 0

    for index, row in tools_df.iterrows():
        if "NOT FOR PROFIT USE" in str(
            tools_df["datasetv2_accessibility_usage_dataUseLimitation"][index]
        ) or "NOT FOR PROFIT USE" in str(
            tools_df["datasetv2_accessibility_usage_dataUseRequirements"][index]
        ):
            nfp_rows.append(row.values)
            nfp_count += 1

    nfp_datasets_df = nfp_datasets_df.append(
        pd.DataFrame(nfp_rows, columns=nfp_datasets_df.columns)
    ).reset_index()

    nfp_datasets_df = filter_df(nfp_datasets_df)

    nfp_datasets_df.to_csv(export_path, index=False)


def main():
    # Client Connection
    db = client_connect(DATABASE_URL, DATABASE)

    tools = get_collection("tools", db)

    COMMERCIAL_QUERY = [
        {"$match": {"type": "dataset", "activeflag": "active"}},
        {
            "$project": {
                "pid": 1,
                "datasetfields": {
                    "datautility": {"allowable_uses": 1, "title": 1},
                    "metadataschema": {"url": 1},
                },
                ""
                "datasetv2": {
                    "accessibility": {
                        "usage": {"dataUseLimitation": 1, "dataUseRequirements": 1}
                    },
                    "summary": {
                        "publisher": {"name": 1, "memberOf": 1},
                    },
                },
            }
        },
    ]

    tools_filtered_list = get_aggregation(tools, COMMERCIAL_QUERY)

    tools_flattened_list = flatten_collection_list(tools_filtered_list)

    tools_df = pd.DataFrame(tools_flattened_list)

    # Export Datasets
    tools_df.to_csv("gateway-datasets.csv", index=False)

    get_commercial_datasets(tools_df, "commercial-datasets.csv")

    not_for_profit_datasets(tools_df, "non-commercial-datasets.csv")


if __name__ == "__main__":
    main()
