import datetime
import os
import sys
import argparse
import requests
import json
import hashlib
import logging

def main(args):
    timestring_format = "%Y-%m-%dT%H:%M:%S.%f"
    loglevel = os.getenv("CETUS_LOGLEVEL",logging.INFO)
    logger = logging.getLogger(__name__)
    logger.setLevel(loglevel)
    with open("api_key", "r") as f:
        apikey = f.read().strip()
    if not apikey:
        logger.error("No API key provided, please put your api key into the api_key file in the same folder as this script")
        exit(1)
    parser = argparse.ArgumentParser()
    parser.add_argument("search")
    parser.add_argument("--host")
    parser.add_argument("--index", choices=["alerting","dns","certstream"])
    parser.add_argument("--media", default="nvme")
    parser.add_argument("--stdout", action="store_true")
    args = parser.parse_args()
    index = args.index
    curtime = datetime.datetime.now()
    since_suffix = ""
    marker_id = None
    end = False
    pit_id = None
    last_id = None
    latest_timestamp = None
    out_data = []
    if os.path.exists(f"{index}_marker"):
        with open(f"{index}_marker", "r") as marker:
            marker_data = json.loads(marker.read())
            if args.search in marker_data:
                marker_search_data = marker_data[args.search]
                marker_string = marker_search_data["last_timestamp"]
                marker_id = marker_search_data["last_uuid"]
                since_suffix = f" AND {index}_timestamp:[{marker_string} TO *]"
                logger.debug(f"Pulling data since {marker_string} and id {marker_id}")
    while not end:
        obj = slurp(apikey, args, since_suffix, pit_id)

        response_data = obj['data']
        ctr = 0
        if marker_id:
            for item in response_data:
                ctr += 1
                if item["uuid"] == marker_id:
                    break
        if ctr == len(response_data):
            # Only record(s) returned ends with our marker record
            return
        out_data.extend(response_data[ctr:])
        end = len(response_data) < 10000
        last_id = out_data[-1]["uuid"]

        latest_timestamp = out_data[-1][f'{index}_timestamp']
        if not end:
            since_suffix = f" AND {index}_timestamp:[{latest_timestamp} TO *]"
            pit_id = obj['pit_id']
    if args.stdout:
        print(json.dumps(out_data, indent=4))
    else:
        outfilename = f"{index}_results_{curtime.timestamp()}.out"
        with open(outfilename, "w") as output:
            logger.info(f"writing results to {outfilename}.  To write to stdout instead, pass --stdout argument")
            output.write(json.dumps(out_data))

        with open(f"{index}_marker", "w") as marker:
            marker.write(json.dumps({args.search:{"last_timestamp": latest_timestamp, "last_uuid": last_id}}))


def slurp(apikey, args, since_suffix, pit_id=None):
    url = f"https://{args.host}/api/query?q={args.search}{since_suffix}&index={args.index}&media={args.media}"
    req_body = None
    if pit_id:
        req_body = {"pit_id": pit_id}
    r = requests.get(url, headers={"Authorization": f"Token {apikey}", "Accept": "application/json"}, data=req_body)
    obj = r.json()
    return obj


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main(sys.argv)




