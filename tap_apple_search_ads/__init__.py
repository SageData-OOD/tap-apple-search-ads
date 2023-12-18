#!/usr/bin/env python3
import os
import json
import re
import backoff
import pytz
import requests
from datetime import datetime, timedelta

import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from singer.transform import transform
from .access_token import AccessToken
from .client_secret import ClientSecret
from .request_headers import RequestHeaders

HOST_URL = "https://api.searchads.apple.com/api/{version}"
OAUTH_URL = "https://appleid.apple.com/auth/oauth2/token"
AUDIENCE = "https://appleid.apple.com"

REQUIRED_CONFIG_KEYS = ["start_date", "end_date", "org_id", "private_key_file"]
DEFAULT_CONVERSION_WINDOW = 14
LOGGER = singer.get_logger()
ENDPOINTS = {
    "organizations": "/acls",
    "ad_groups": "/campaigns/{campaignId}/adgroups",
    "campaigns": "/campaigns",
    "ad_level_reports": "/reports/campaigns/{campaignId}/ads",
    "ad_group_level_reports": "/reports/campaigns/{campaignId}/adgroups",
    "campaign_level_reports": "/reports/campaigns",
    "search_term_level_reports": "/reports/campaigns/{campaignId}/searchterms"
}


class AppleAdsRateLimitError(Exception):
    def __init__(self, msg):
        self.msg = msg
        super().__init__(self.msg)


class TapAppleSearchAdsException(Exception):
    pass


def set_query_object(stream_id):
    mapping = {
        "ad_level_reports": "adId",
        "ad_group_level_reports": "adGroupId",
        "campaign_level_reports": "campaignId",
        "search_term_level_reports": "keywordId"
    }
    query = {
        "selector": {
            "orderBy": [{"field": mapping[stream_id], "sortOrder": "ASCENDING"}],
        },
        "timeZone": "UTC",
        "granularity": "DAILY",
        "returnRecordsWithNoMetrics": False
    }

    return query


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def generate_id(row, stream_id):
    date = str(row["metrics"]["date"])
    campaign_id = str(row["metadata"].get("campaign_id"))
    ad_group_id = str(row["metadata"].get("ad_group_id"))
    ad_id = str(row["metadata"].get("ad_id"))
    keyword_id = str(row["metadata"].get("keyword_id"))

    key_properties = {
        "ad_level_reports": [date, campaign_id, ad_group_id, ad_id],
        "ad_group_level_reports": [date, campaign_id, ad_group_id],
        "campaign_level_reports": [date, campaign_id],
        "search_term_level_reports": [date, campaign_id, keyword_id]
    }
    return "#".join(key_properties[stream_id])


def get_key_properties(stream_id):
    key_properties = {
        "ad_groups": ["id"],
        "campaigns": ["id"],
        "organizations": ["org_id"]
    }
    return key_properties.get(stream_id, ["record_id"])


def get_bookmark(stream_id):
    bookmark = {
        "ad_level_reports": "date",
        "ad_group_level_reports": "date",
        "campaign_level_reports": "date",
        "search_term_level_reports": "date"
    }
    return bookmark.get(stream_id)


def get_properties_for_auto_inclusion(stream_id, key_properties, replication_key):
    properties = {
        "ad_groups": ["id"],
        "campaigns": ["id"],
        "organizations": ["org_id"],
        "ad_level_reports": ["date", "campaign_id", "ad_group_id", "ad_id"],
        "ad_group_level_reports": ["date", "campaign_id", "ad_group_id"],
        "campaign_level_reports": ["date", "campaign_id"],
        "search_term_level_reports": ["date", "campaign_id", "keyword_id"]
    }
    return properties[stream_id] + key_properties + [replication_key]


def create_metadata_for_report(stream_id, schema, key_properties):
    replication_key = get_bookmark(stream_id)
    mdata = [{"breadcrumb": [], "metadata": {"inclusion": "available", "forced-replication-method": "INCREMENTAL",
                                             "valid-replication-keys": [replication_key]}}]
    if key_properties:
        mdata[0]["metadata"]["table-key-properties"] = key_properties

    if replication_key is None:
        mdata[0]["metadata"]["forced-replication-method"] = "FULL_TABLE"
        mdata[0]["metadata"].pop("valid-replication-keys")

    auto_inclusion = get_properties_for_auto_inclusion(stream_id, key_properties, replication_key)
    for key in schema.properties:
        # hence, when property is object, we will only consider properties of that object without taking object itself.
        if "object" in schema.properties.get(key).type:
            for prop in schema.properties.get(key).properties:
                inclusion = "automatic" if prop in auto_inclusion else "available"
                mdata.append({"breadcrumb": ["properties", key, "properties", prop],
                              "metadata": {"inclusion": inclusion}})

        else:
            inclusion = "automatic" if key in auto_inclusion else "available"
            mdata.append({"breadcrumb": ["properties", key], "metadata": {"inclusion": inclusion}})

    return mdata


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():

        # 404-not found for "ad_level_reports" endpoint [hence, skipping it for now]
        if stream_id == "ad_level_reports":
            continue

        stream_metadata = create_metadata_for_report(stream_id, schema, get_key_properties(stream_id))
        key_properties = get_key_properties(stream_id)
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata
            )
        )
    return Catalog(streams)


def snake_to_camel_case(element):
    return ''.join(ele.title() for ele in element.split("_"))


def camel_to_snake_case(name):
    """
    AssimilatedVatBox  --> assimilated_vat_box
    """
    exceptional = {
        "avg_c_p_a": "avg_cpa",
        "avg_c_p_m": "avg_cpm",
        "avg_c_p_t": "avg_cpt"
    }
    sn = re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
    sn = sn.split(" ")[0]  # i.e. "duration (in second)" -> "duration"
    return exceptional.get(sn, sn)


def refactor_property_name(record):
    if isinstance(record, list):
        return [refactor_property_name(r) if isinstance(r, (dict, list)) else r for r in record]
    else:
        converted_data = {camel_to_snake_case(k): v if not isinstance(v, (dict, list)) else refactor_property_name(v)
                          for k, v in record.items()}
        return converted_data


def get_api_version(config):
    return "v4" if config["auth_type"] == "oauth2" else "v3"


def get_certificates(config):
    # in certificate bases authentication, we need private as well pem value
    if config["auth_type"] == "certificate":
        return config["pem_file"], config["private_key_file"]
    return None


@backoff.on_exception(backoff.expo, AppleAdsRateLimitError, max_tries=5, factor=2)
@utils.ratelimit(1, 1)
def request_data(config, headers, endpoint, attr=None, query=None, campaign_id=None):
    v = get_api_version(config)
    url = HOST_URL.format(version=v) + endpoint.format(campaignId=campaign_id)

    cert = get_certificates(config)
    # every report will have query object
    if query:
        response = requests.post(url, headers=headers, json=query, cert=cert)

        if response.status_code == 429:
            raise AppleAdsRateLimitError(response.text)
        elif response.status_code != 200:
            raise Exception(response.text)

        data = response.json().get("data", {}).get("reportingDataResponse", {}).get("row")
        return [data] if isinstance(data, dict) else data

    # Non-report streams
    else:
        new_url = url
        results = []

        while True:
            attr["offset"] = len(results)
            if attr:
                new_url = url + "?" + "&".join([f"{k}={v}" for k, v in attr.items()])
            response = requests.get(new_url, headers=headers, cert=cert)

            if response.status_code == 429:
                raise AppleAdsRateLimitError(response.text)
            elif response.status_code != 200:
                raise Exception(response.text)

            results += response.json().get("data", [])
            pagination = response.json().get("pagination")
            total_results = pagination.get("totalResults", {}) if pagination else len(results)

            if len(results) == total_results:
                break
        return results


def get_end_date(start_date, until_date):
    end_date = datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=30)

    # if end_date bigger than until_date date, then end_time = current datetime
    if end_date.strftime('%Y-%m-%d') > until_date:
        end_date = datetime.strptime(until_date, "%Y-%m-%d")
    return end_date.strftime("%Y-%m-%d")


def get_campaigns(stream_id, config, headers):
    if stream_id in ["campaigns", "campaign_level_reports", "organizations"]:
        return [{"id": None}]

    endpoint = ENDPOINTS["campaigns"]
    attr = {"limit": 100, "offset": 0}
    campaigns = request_data(config, headers, endpoint, attr=attr)
    return campaigns


def refactor_records(tap_data):
    # as we receive multiple metrics objects for single metadata(dimension)
    records = []
    for row in tap_data:
        records += [{
            "insights": row.get("insights"),
            "metadata": row.get("metadata"),
            "other": row.get("other"),
            "metrics": metric
        } for metric in row.get("granularity", [])]
    return records


def get_valid_start_date(date_to_poll, conversion_window):
    """
    fix for data freshness
    e.g. Sunday's data is available at 3 AM UTC on Monday
    If integration is set to sync at 1AM then a problem occurs
    """

    utcnow = datetime.utcnow()
    date_to_poll = datetime.strptime(date_to_poll, "%Y-%m-%d")

    if date_to_poll >= utcnow - timedelta(days=conversion_window):
        date_to_poll = utcnow - timedelta(days=conversion_window)

    return date_to_poll.strftime("%Y-%m-%d")


def sync_reports(config, state, stream, headers=None):
    bookmark_column = get_bookmark(stream.tap_stream_id)
    mdata = metadata.to_map(stream.metadata)
    schema = stream.schema.to_dict()

    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=schema,
        key_properties=stream.key_properties,
    )

    bookmark = singer.get_bookmark(state, stream.tap_stream_id, bookmark_column) \
        if state.get("bookmarks", {}).get(stream.tap_stream_id) else config["start_date"]

    conversion_window = config.get("conversion_window", DEFAULT_CONVERSION_WINDOW)
    start_date = get_valid_start_date(bookmark, conversion_window)

    query = set_query_object(stream.tap_stream_id)
    endpoint = ENDPOINTS[stream.tap_stream_id]
    campaigns = get_campaigns(stream.tap_stream_id, config, headers)

    while True:
        query["startTime"] = start_date
        end_date = get_end_date(start_date, config["end_date"])

        if start_date == end_date:
            break

        query["endTime"] = end_date
        LOGGER.info("Querying ----> %s, Date --> %s to %s", stream.tap_stream_id, start_date, end_date)

        # for campaign_level_reports, campaign_ids=[None]
        # will fetch records of all campaign_ids for specific time chunk, in a loop until it reaches to the until_date.
        for campaign in campaigns:
            if "APPSTORE_TODAY_TAB" in campaign.get("supplySources", []) and stream.tap_stream_id == "search_term_level_reports":
                continue

            cid = campaign["id"]

            LOGGER.info("Querying -> %s for campaign_id -> %s", stream.tap_stream_id, cid)
            tap_data = request_data(config, headers, endpoint, query=query, campaign_id=cid)

            with singer.metrics.record_counter(stream.tap_stream_id) as counter:
                refactored_records = refactor_records(tap_data)
                for row in refactored_records:
                    if cid is not None:
                        row["metadata"]["campaign_id"] = cid

                    row = refactor_property_name(row)
                    row["record_id"] = generate_id(row, stream.tap_stream_id)

                    # Type Conversation and Transformation
                    transformed_data = transform(row, schema, metadata=mdata)

                    # write one or more rows to the stream:
                    singer.write_records(stream.tap_stream_id, [transformed_data])
                    counter.increment()

        state = singer.write_bookmark(state, stream.tap_stream_id, bookmark_column, end_date)
        singer.write_state(state)

        start_date = end_date


def sync_endpoints(config, state, stream, headers=None):
    """ For full sync operation for campaigns and ad_groups (No QSP available to perform incremental sync) """
    mdata = metadata.to_map(stream.metadata)
    schema = stream.schema.to_dict()

    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=schema,
        key_properties=stream.key_properties,
    )

    endpoint = ENDPOINTS[stream.tap_stream_id]
    campaigns = get_campaigns(stream.tap_stream_id, config, headers)
    attr = {"limit": 100, "offset": 0}

    LOGGER.info("Querying -------> %s", stream.tap_stream_id)
    for campaign in campaigns:
        cid = campaign["id"]
        LOGGER.info("Querying -> %s for campaign_id -> %s", stream.tap_stream_id, cid)
        tap_data = request_data(config, headers, endpoint, attr=attr, campaign_id=cid)

        with singer.metrics.record_counter(stream.tap_stream_id) as counter:
            for row in tap_data:
                # Type Conversation and Transformation
                row = refactor_property_name(row)
                transformed_data = transform(row, schema, metadata=mdata)

                # write one or more rows to the stream:
                singer.write_records(stream.tap_stream_id, [transformed_data])
                counter.increment()


def read_key_from_file(key_path) -> str:
    with open(key_path, "r") as key_file:
        key = "".join(key_file.readlines()).strip()

    return key


def load_private_key(config) -> str:
    if "private_key_file" in config:
        private_key_file = config["private_key_file"]
        private_key = read_key_from_file(private_key_file)
    else:
        raise TapAppleSearchAdsException("Missing private key configuration parameters")

    return private_key


def set_up_authentication(timestamp, config):
    client_secret_ = ClientSecret(timestamp, config["expiration_time"])
    client_secret_.set_headers(config["key_id"], config["algorithm"])
    client_secret_.set_payload(config["client_id"], config["team_id"], config["audience"])

    access_token_ = AccessToken(config["client_id"], config["oauth_url"])

    request_headers_ = RequestHeaders(config["org_id"])
    return client_secret_, access_token_, request_headers_


def get_request_headers(config):
    if config["auth_type"] == "oauth2":
        now = datetime.now(tz=pytz.utc)
        timestamp = int(now.timestamp())
        cs, at, rh = set_up_authentication(timestamp, config)
        private_key = load_private_key(config)
        return rh.value(at.value(cs.value(private_key)))
    else:
        return {"Content-Type": "application/json", "Authorization": f"orgId={config['org_id']}"}


def set_up_config(config):
    config["algorithm"] = "ES256"
    config["audience"] = AUDIENCE
    config["oauth_url"] = OAUTH_URL
    config["expiration_time"] = 43200  # 12hour


def sync(config, state, catalog):
    set_up_config(config)
    request_headers_value = get_request_headers(config)

    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        if stream.tap_stream_id in ["ad_level_reports", "ad_group_level_reports", "campaign_level_reports",
                                    "search_term_level_reports"]:
            sync_reports(config, state, stream, headers=request_headers_value)
        else:
            sync_endpoints(config, state, stream, headers=request_headers_value)


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
