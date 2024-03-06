from typing import List
from pandas.core.generic import json
import requests
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ThreadPoolExecutor   
from os import listdir
from os.path import isfile, join 

# Number of threads to use for parallel execution
num_threads = 16 

member_url = 'https://lso.ca/public-resources/finding-a-lawyer-or-paralegal/directory-search/member'

def get_proccessed_files() -> List[str]:
    path = './lso/output/'
    files = [f.split(".")[0] for f in listdir(path) if isfile(join(path, f))]
    return files

def read_numbers() -> List[str]:
    """
        Reads input file of all the available lawyer numbers
        
        Returns 
            :return: List of strings containing lawyer numbers
    """
    with open('./lso/lawyer_numbers.txt', mode='r', encoding='utf-8') as f:
        return f.read().splitlines()

def fetch_lawyer(number: str):
    """
        Fetches the html content for each lawyer

        Args:
            :number: string - Lawyer number

        Returns:
            :return: html page with laywer details
    """    
    # have to spoof User-Agent since python-requests get blocked
    headers = {
        'User-Agent': 'PostmanRuntime/7.36.3',
        "Cookie": 'ASP.NET_SessionId=aij42hbivghdhlw23syo54p5; CMSPreferredCulture=en-CA',
    }
    # url = member_url + f"?MemberNumber={number}"
    response = requests.get(member_url, params={"MemberNumber": number},headers=headers)

    return response.text


def parse_lawyer(number: str):
    """
        Parses html page for lawyers and extracts available data 

        Args:
            :number: string - Lawyer number

        Returns:
            :return: dict containing extracted laywer info
    """
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
    with open(f"./lso/output/{number}.json", "w") as f:
        json.dump(member_dict, f)
    # return member_dict
    


if __name__ == "__main__":
    lawyer_numbers = set(read_numbers())
    # run the parsing concurrently    
    processed_files = set(get_proccessed_files())
    # print (len(get_proccessed_files()))
    lawyers_to_process = lawyer_numbers - processed_files
    print(len(lawyer_numbers))
    print(len(lawyers_to_process))
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Submit tasks for parallel execution
        futures = [executor.submit(parse_lawyer, i) for i in lawyers_to_process]

        # # Wait for all tasks to complete and get results
        # lawyer_data_list = [future.result() for future in futures]
        # 
        # lawyer_data_df = pd.DataFrame(lawyer_data_list)
        #
        #
        # lawyer_data_df.to_csv(f"lawyer_sample.csv", header=True)
