import argparse
from operator import index
import requests
import pandas as pd
from pandas import json_normalize

BASE_URL = "https://fv0xpb3eg4-dsn.algolia.net/1/indexes/crs_production/query"
HITS_PER_PAGE = 20

def parse_args():
    parser = argparse.ArgumentParser(
        prog="Lawyer Firm Directory Parser",
        description="Parses lawyer firm information based on provided search criteria"
        )
   
    parser.add_argument("--out", default="firms.csv", help="Output filename (default: firms.csv)")
    parser.add_argument("--api_key", required=True, help="API key from www.bcfsa.ca")
    parser.add_argument("--app_id", required=True, help="App ID from www.bcfsa.ca")
    parser.add_argument("--search", default="**", help="City search query(default: **)")
    

    args = parser.parse_args()
    
    return {
        "outfile": args.out,
        "api_key": args.api_key,
        "app_id": args.app_id,
        "search": args.search,
    }

def parse_json_response(obj, df):

    # drop unwanted keys 
    required_keys = ["license_number","expiry_date", "services", "subtype","services", "name", "secondary_name", 'location', "address"]
    filtered_dict = {k: v for k, v in obj.items() if k in required_keys}
    new_df = json_normalize(filtered_dict)
    return pd.concat([df, new_df])


if __name__ == "__main__":
    args = parse_args()

     
    
    request_body = {
        "query": args["search"],
        "filters": "bundle:re_brokerage",
        "hitsPerPage": HITS_PER_PAGE,
        "restrictSearchableAttributes": ["location"], #Enable search for locations only, remove this to disable
    }
    page = 0
    print(request_body)
    more_results_to_fetch = True
    df = pd.DataFrame()
    while (more_results_to_fetch):
        request_body["page"] = page
        res = requests.post(f"{BASE_URL}?x-algolia-api-key={args['api_key']}&x-algolia-application-id={args['app_id']}", json=request_body) 
        res_obj = res.json()
        for obj in res_obj["hits"]:
            df = parse_json_response(obj, df)
         
        print("Processed page:",res_obj['page'])
        if (page == res_obj['nbPages'] - 1):
            more_results_to_fetch = False
        page += 1 

    df.to_csv(args["outfile"], header=True, index=False )
