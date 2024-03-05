from typing import List
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import string
    

#https://lso.ca/public-resources/finding-a-lawyer-or-paralegal/directory-search/member?MemberNumber=P15497
member_url = 'https://lso.ca/public-resources/finding-a-lawyer-or-paralegal/directory-search/member'

def read_numbers() -> List[str]:
    with open('./lso/lawyer_numbers.txt', mode='r', encoding='utf-8') as f:
        return f.read().splitlines()

def fetch_lawyer(number: str):
    # have to spoof User-Agent since python-requests get blocked
    headers = {
        'User-Agent': 'PostmanRuntime/7.36.3',
        "Cookie": 'ASP.NET_SessionId=aij42hbivghdhlw23syo54p5; CMSPreferredCulture=en-CA',
    }
    # url = member_url + f"?MemberNumber={number}"
    response = requests.get(member_url, params={"MemberNumber": number},headers=headers)

    return response.text

def clean_string(text):
    """Replaces newline and carriage return characters in the middle of the string with commas,
    and removes them from the beginning and end of the string.

    Args:
      text: The string to be processed.

    Returns:
      The processed string.
    """
    l_stripped = text.lstrip()  # Remove newline and carriage return characters from the beginning and end
    r_stripped = l_stripped.rstrip()    
    print(r_stripped)
    return r_stripped

def parse_lawyer(number: str): #-> pd.DataFrame
    print(number)
    lawyer_page = fetch_lawyer(number)
    soup = BeautifulSoup(lawyer_page, "html.parser")
    
    title = soup.find('h2', {"class": "member-info-title"})
    member_info_parent = soup.find("div", {"class": "member-information"})
    # Ignore errors
    member_info = member_info_parent.findChildren("div", {"class": "member-info-wrapper"})\
        + member_info_parent.findChildren("div", {"class": "member-special-cases"})
    member_dict = {}
    for field in member_info:
        label: str = field.find(class_="member-info-label").text
        value= field.find(class_="member-info-value")
        if value is None:
           value = field.getText(strip=True, separator=",").split(",")[1]
        else:
            value = value.getText(strip=True, separator=', ')
        member_dict[label] =value 

    # print(member_dict)
    return member_dict


if __name__ == "__main__":
    lawyer_numbers = read_numbers()
   
    lawyer_data_list = [parse_lawyer(i) for i in lawyer_numbers[0:100]]
    lawyer_data_df = pd.DataFrame(lawyer_data_list)

    lawyer_data_df.to_csv(f"lawyer_sample.csv", header=True)
    # df.to_csv(f"{output_file}", header=True, index=False)
    # lawyer_data_df
