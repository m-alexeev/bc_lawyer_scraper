from typing import List 
import requests
import json



URL = "https://lawsocietyontario.search.windows.net/indexes/lsolpindexprd/docs/search?api-version=2017-11-11"

#Search Params
body = {"search":"*",
        "count":True,
        "filter":"search.ismatch('/.***.*/', 'memberfirstname,membermiddlename,membermailname,memberfirstnameclean,membermiddlenameclean,membermailnameclean', 'full', 'all')",
        "orderby":"memberlastname, memberfirstname, membermiddlename",
        "queryType":"full",
        "searchFields":"memberfirstname"}

# Azure api key 
headers = {
    "Content-Type": "application/json",
    "Api-Key":"212D535962D4563E62F8EC5D6E1C71CA"
}

def fetch_numbers(count: int, skip: int) -> tuple[int, List[str]]: 
    body['top'] = count
    body["skip"] = skip
    
    response = requests.post(URL,data=json.dumps(body), headers=headers)

    count = 0
    lawyer_numbers = []
    if response.status_code == 200:
        # Automatically decode the response content based on Content-Encoding header
        res = response.json()
        count = len(res["value"])
        lawyer_numbers = [lawyer["membernumber"] for lawyer in res["value"]]
    elif response.status_code == 400:
        print(response.json())
    else:
        print("Failed to retrieve data from the endpoint. Status code:", response.status_code)

    return count, lawyer_numbers

if __name__ == "__main__":
    num_members_to_fetch = 1000
    start = num_members_to_fetch 
    skip = 0
    
    # Fetch lawyers 
    fetched,numbers = fetch_numbers(num_members_to_fetch, skip)
    lawyer_numbers = numbers
    while fetched != 0:
        start = start + num_members_to_fetch 
        skip = skip + num_members_to_fetch
        fetched, numbers = fetch_numbers(start, skip )
        lawyer_numbers = lawyer_numbers + numbers


    # Save to file
    with open('./lso/lawyer_numbers.txt', mode='wt', encoding='utf-8') as myfile:
        myfile.write('\n'.join(lawyer_numbers))
