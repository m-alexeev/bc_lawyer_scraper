import argparse
from re import search
import requests
from requests import Session
from bs4 import BeautifulSoup
from lawyer import Lawyer 
import pandas as pd


BASE_URL = "https://www.lawsociety.bc.ca/lsbc/apps/lkup/"
ADDITIONAL_PARAMS = "&member_search=Search&is_submitted=1&results_no=50"

def parse_args():
    parser = argparse.ArgumentParser(
        prog="Lawyer Directory Parser",
        description="Parses lawyer information based on provided search criteria"
        )
   
    parser.add_argument("--out", default="outfile.csv", help="Output filename (default: outfile.csv)")
    parser.add_argument("--session_id", required=True, help="JSession ID from an active lawsociety.bc.ca session")
    parser.add_argument("--last_name", default="**", help="Last name (default: **)")
    parser.add_argument("--first_name", default="", help="First name (default: none)")
    parser.add_argument("--match", choices=["contains", "starts-with", "exact-match"],
                        default="contains", help="Last name matching criteria (default: contains)")
    parser.add_argument("--city",default="", help="Search city (default: none)")
     
    match_choices = {
        "contains": 2, 
        "starts-with": 1,
        "exact-match": 3
    }

    args = parser.parse_args()
    
    return {
        "outfile": args.out,
        "session": args.session_id,
        "txt_last_nm": args.last_name,
        "txt_search_type": match_choices[args.match],
        "txt_city": args.city,
        "txt_given_nm": args.first_name,
    }

def cleanse_address(address: str) -> str:
    split_address_str = address.split("\n")
    isolate_address = split_address_str[:4]
    clean_address = " ".join([add.strip("\r").strip() for add in isolate_address])
    return clean_address


def clease_phone(phone: str) -> str:
    phone = "-".join(phone.strip().split(" ")[:2])
    clean_phone = phone.strip()
    return clean_phone


# Name, contact information (phone number & address)
def parse_lawyer(url: str, session: Session) -> Lawyer | None:
    resp = session.get(f"{BASE_URL}/{url}")
    if (resp.status_code==200):
        html = resp.content
        soup = BeautifulSoup(html, "html.parser")
        # lawyer_dc = Lawyer(soup.find("h3"))
        try: 
            name = soup.find("h3").text
            # Get first 10 data points since last points are useless
            form_data = soup.find_all("div", class_="col-sm-9")[:7]
            lawyer_data = []
            if (form_data[0].text.strip() != "Practising"):
                print("Lawyer is not practicing, skipping...")
                return None
            
            for index, label in enumerate(form_data):
    
                if index == 4 or index == 5:
                    lawyer_data.append(clease_phone(label.text))
                elif index == 3:
                    lawyer_data.append(cleanse_address(label.text))
                else:
                    lawyer_data.append(label.text.strip())
            lawyer = Lawyer(name, lawyer_data[0], lawyer_data[1], lawyer_data[2], lawyer_data[3], lawyer_data[4], lawyer_data[5])
            return lawyer
        except Exception as e:
            print(e)
            print(f"Failed to parse lawyer information at {BASE_URL}/{url}")
    else:
        print(resp.status_code)
        print(f"Cannot find lawyer with {BASE_URL}/{url}")



if __name__ == "__main__":
    params = parse_args()

    cookie = {"JSESSIONID": params.get("session"), "CFTOKEN":"33846498", "CFID":"3303238"}

    session = requests.Session()
    session.cookies.update(cookie)

    output_file = params["outfile"]

    del params["session"]
    del params["outfile"]

    query_string ='&'.join([f"{k}={v}" for k, v in params.items()])

    response = requests.get(f"{BASE_URL}mbr-search.cfm?{query_string}{ADDITIONAL_PARAMS}")
    lawyer_dicts = []
    if response.status_code == 200: 
        page_html = response.content
        soup = BeautifulSoup(page_html, "html.parser")
         
        rows = soup.find_all('td',{"data-title": "Name"})
        links = []
        for r in rows: 
            links.append(r.a.get("href"))

        for index, link in enumerate(links):
            data = parse_lawyer(link, session)
            if data is not None:
                lawyer_dicts.append(data.__dict__)
    else:
        print('Failed to fetch data with these request params')

    # Save file 
    df = pd.DataFrame(lawyer_dicts)
    df.to_csv(f"{output_file}", header=True, index=False)
