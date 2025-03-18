import datetime
import os
import sys
import argparse
import requests
import json
import hashlib
import logging

loglevel = os.getenv("CETUS_LOGLEVEL", logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(loglevel)

def main(args):
    timestring_format = "%Y-%m-%dT%H:%M:%S.%f"

    apikey = get_apikey()
    parser = argparse.ArgumentParser()
    parser.add_argument("search")
    # parser.add_argument("--host")
    parser.add_argument("--index", default="dns", choices=["alerting", "dns", "certstream"])
    parser.add_argument("--media", default="nvme", choices=["nvme", "all"])
    parser.add_argument("--stdout", action="store_true")
    parser.add_argument("--since-days", help="How many days back to look.  Only has an effect on first pull", default=7,
                        type=int)
    args = parser.parse_args()
    # hostname = args.host
    index = args.index
    curtime = datetime.datetime.now()

    marker_id = None
    since_days = args.since_days
    since_suffix = None
    search = args.search
    media = args.media

    if os.path.exists(f"{index}_marker"):
        with open(f"{index}_marker", "r") as marker:
            marker_data = json.loads(marker.read())
            if args.search in marker_data:
                marker_search_data = marker_data[args.search]
                marker_string = marker_search_data["last_timestamp"]
                marker_id = marker_search_data["last_uuid"]
                since_suffix = f" AND {index}_timestamp:[{marker_string} TO *]"
                logger.debug(f"Pulling data since {marker_string} and id {marker_id}")

    out_data, last_id, latest_timestamp = query(apikey, index, search, media, since_days, since_suffix, marker_id)
    if args.stdout:
        print(json.dumps(out_data, indent=4))
    else:
        outfilename = f"{index}_results_{curtime.timestamp()}.out"
        with open(outfilename, "w") as output:
            logger.info(f"writing results to {outfilename}.  To write to stdout instead, pass --stdout argument")
            output.write(json.dumps(out_data))

        with open(f"{index}_marker", "w") as marker:
            marker.write(json.dumps({args.search: {"last_timestamp": latest_timestamp, "last_uuid": last_id}}))


def get_apikey():
    with open("api_key", "r") as f:
        apikey = f.read().strip()
    if not apikey:
        logger.error(
            "No API key provided, please put your api key into a file called \"api_key\" in the same folder as this script")
        exit(1)
    return apikey


def     query(apikey, index, search, media="nvme",since_days=7, since_suffix=None, marker_id=None):
    pit_id = None
    end = False
    last_id = None
    hostname = "alerting.sparkits.ca"
    latest_timestamp = None
    last_id=None

    out_data = []
    index_timestamp_field = "timestamp"
    if index != "certstream":
        index_timestamp_field = f"{index}_timestamp"
    if not since_suffix:
        since_suffix = f" AND {index_timestamp_field}:[{(datetime.datetime.today() - datetime.timedelta(days=since_days)).replace(microsecond=0).isoformat()} TO *]"
    while not end:
        obj = slurp(apikey, search, index, media, since_suffix, hostname, pit_id)
        response_data = obj['data']
        ctr = 0

        if marker_id:
            for item in response_data:
                ctr += 1
                if item["uuid"] == marker_id:
                    marker_id=None
                    break

        if ctr == len(response_data):
            # Only record(s) returned ends with our marker record
            break

        out_data.extend(response_data[ctr:])
        end = len(response_data) < 10000
        last_id = out_data[-1]["uuid"]
        latest_timestamp = out_data[-1][f'{index_timestamp_field}']

        if not end:
            since_suffix = f" AND {index_timestamp_field}:[{latest_timestamp} TO *]"
            pit_id = obj['pit_id']

    return out_data, last_id, latest_timestamp


def slurp(apikey, search, index, media, since_suffix, hostname, pit_id=None):
    #url = f"https://{hostname}/api/query?query={search}{since_suffix}&index={index}&media={media}"
    url = f"https://{hostname}/api/query/"
    req_body = {
        "query": f"{search}{since_suffix}",
        "index": index,
        "media": media
    }
    # req_body = None
    if pit_id:
        req_body["pit_id"]= pit_id
    r = requests.post(url, headers={"Authorization": f"Token {apikey}", "Accept": "application/json"}, data=req_body)
    obj = r.json()
    return obj


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main(sys.argv)
