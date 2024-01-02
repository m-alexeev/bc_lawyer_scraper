import argparse
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
from pandas import json_normalize
import concurrent.futures

BASE_URL = "https://fv0xpb3eg4-dsn.algolia.net/1/indexes/crs_production/query"
BROKER_BASE_URL = 'https://www.bcfsa.ca/re-brokerage'
HITS_PER_PAGE = 20

NUM_WORKERS = 12


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
    required_keys = ["licence_number","expiry_date", "services", "subtype","services", "name", "secondary_name", 'location', "address"]
    filtered_dict = {k: v for k, v in obj.items() if k in required_keys}
    new_df = json_normalize(filtered_dict)
    return pd.concat([df, new_df])

def split_address(row):
    try:
        return pd.Series(row.split(',')[:2])
    except :
        return pd.Series([None, None])


def get_realty_data(df):
    res = requests.get(f'{BROKER_BASE_URL}/{df['licence_number']}')
    soup = BeautifulSoup(res.content, "html.parser")
    # Address row 0 col 1
    # Get Business Address
    business_address_tag = soup.find('dt', string='Business Address:')
    business_address = ' '.join(business_address_tag.find_next('dd').string.strip().split(' ')[-2:]) if business_address_tag else None

    # Get Business Phone
    business_phone_tag = soup.find('dt', string='Business Phone:')
    business_phone = business_phone_tag.find_next('dd').string.strip() if business_phone_tag else None

    # Get Business Fax
    business_fax_tag = soup.find('dt', string='Business Fax:')
    business_fax = business_fax_tag.find_next('dd').string.strip() if business_fax_tag else None

    
    df['postal_code'] = business_address
    df['business_phone'] = business_phone
    df['business_fax'] = business_fax
    return df

def process_parallel(df_chunk):
    return df_chunk.apply(get_realty_data, axis=1)


if __name__ == "__main__":
    args = parse_args()
    
    request_body = {
        "query": args["search"],
        "filters": "bundle:re_brokerage",
        "hitsPerPage": HITS_PER_PAGE,
        "restrictSearchableAttributes": ["location"], #Enable search for locations only, remove this to disable
    }
    page = 0
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
    
    # format expiry date
    df['expiry_date'] = pd.to_datetime(df['expiry_date'], unit='s').dt.date
    # Split Location into multiple columns
    df_add = df['address']
    df_2 = df['address'].apply(split_address)
    # Rename the split columns
    df_2 = df_2.rename(columns={0: 'street_address', 1: "city"})
    
    merged_df = pd.concat([df, df_2.reindex(df.index)], axis=1)
    
    # Split the dataframes into chunks to process data in parallel
    chunks = np.array_split(merged_df, NUM_WORKERS)
    
    # Fetch data in parallel to avoid waiting for each fetch
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        extra_data_df = list(executor.map(process_parallel, chunks))

    # Merge the dataframes
    resulting_df = pd.concat(extra_data_df)
    resulting_df.to_csv(args["outfile"], header=True, index=False )
