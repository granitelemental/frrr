from sqlalchemy import create_engine
import sqlalchemy
from time import sleep
import pandas as pd
import requests
import xmltodict, json
from bs4 import BeautifulSoup
import re 
from datetime import datetime
import timestring
import json
import numpy as np
import random

# che za hernya???

# https://www.ncbi.nlm.nih.gov/books/NBK25498/#chapter3.Application_3_Retrieving_large 
# and 
# https://www.ncbi.nlm.nih.gov/books/NBK25497/
# to avoid using multiple requests


# def get_pmids(keyword,retstart=1,max_results=100000):    
#     """# gets PMIDs using keyword 
#     # max_results - max number of results """
#     r = requests.get(f"""http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmode=json&retstart={retstart}&retmax={max_results}&sort=uid&term={keyword}""")
#     answer = r.json()
#     pmids = answer["esearchresult"]["idlist"]
#     return(pmids)

def get_pmids(keyword,retstart=1,max_results=100000, sortby="relevance"):    
    """gets PMIDs using keyword 
    max_results - max number of results """
    # if retstart is None:
    #     result = [None, None]
    #     print("retstart is None")
    # else:    
    sleep(0.3)
    r = requests.get(f"""http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmode=json&retstart={retstart}&retmax={max_results}&term={keyword}""") #&sort={sortby}
    answer = r.json()

    pmids = list(map(int, answer["esearchresult"]["idlist"]))
    count = int(answer["esearchresult"]["count"])
    
    result = [pmids, count]
    return(result)    


def count_pubmed_pubmed_citations(pmid):
    """returns number of pubmed_pubmed citations for given pmid"""
    link = f"https://www.ncbi.nlm.nih.gov/pubmed?linkname=pubmed_pubmed_citedin&from_uid={pmid}"
    sleep(0.3)
    page = requests.get(link).text
    soup = BeautifulSoup(page, 'html.parser')  
    count = int(soup.find_all(attrs={"name": "ncbi_resultcount"})[0]["content"])
    return(count)

def count_other_citations(pmid,linkname):
    """returns number of other citations for given pmid
linkname are listed on https://eutils.ncbi.nlm.nih.gov/entrez/query/static/entrezlinks.html"""
    link = f"https://www.ncbi.nlm.nih.gov/pubmed?linkname={linkname}&from_uid={pmid}"
    sleep(0.3)
    page = requests.get(link).text
    soup = BeautifulSoup(page, 'html.parser')  
    count = int(soup.find_all(attrs={"name": "ncbi_resultcount"})[0]["content"])
    return(count)


def get_soup(pmid,retmode="xml"):
    url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id={pmid}&retmode={retmode}&tool=my_tool&email=my_email@example.com"
    sleep(0.3)
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser') 
    return(soup)


def get_journal(soup):
    J_name = soup.find_all(attrs={"name": "FullJournalName"})[0].get_text() 
    return(J_name)

def get_title(soup):
    Art_name = soup.find_all(attrs={"name": "Title"})[0].get_text() 
    return(Art_name)


def get_country_from_string(affiliation_string):
    """Extracts country from affiliation string"""

    affiliation_string = affiliation_string.replace("Electronic address:","")
        
    mail = re.findall("\S+@\S+" , affiliation_string)
    if len(mail)>0:
        for m in mail:
            affiliation_string = affiliation_string.replace(m,"")

    brcts = re.findall("\(.*\)" , affiliation_string)  
    if len(brcts)>0:
        for b in brcts:
            affiliation_string = affiliation_string.replace(b,"")        

    for term in affiliation_string.strip().split(","):
        if term!="":
            country = term.strip()
            
    country = country.strip(".")   
    return(country)


def get_aff_countries(pmid,PI=False):
    """# extracts unique countries from affiliations of either all or last author
    # author = "all" or "last"
    # soup:"""
    link = f'''http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&retmode=xml&id={pmid}'''
    page = requests.get(link).text
    soup = BeautifulSoup(page, 'html.parser') 
    countries = []
    
    if PI==True:
        count=-1
        while len(soup.find_all("author")[count].find_all("lastname"))==0:
            count-=1 
            
        for affiliation in soup.find_all("author")[count].find_all("affiliation"):
            countries.append(get_country_from_string(affiliation.get_text()))
            
    else:
        for auth in soup.find_all("author"):
            for affiliation in auth.find_all("affiliation"):
                countries.append(get_country_from_string(affiliation.get_text())) 
    return(list(pd.unique(countries))) 


def get_PI(soup):
    """# returns name of last author
    # soup:
    # link = f'''http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&retmode=xml&id={pmid}'''
    # art = requests.get(link)
    # soup = BeautifulSoup(art.text, 'html.parser') """
    last_author = soup.find_all(attrs={"name":"LastAuthor"})[0].get_text()
    return(last_author)


def get_n_authors(soup):
    """# returns number of authors
    # soup:
    # link = f'''http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&retmode=xml&id={pmid}'''
    # art = requests.get(link)
    # soup = BeautifulSoup(art.text, 'html.parser')  """
    n_authors = len(soup.find_all(attrs={"name":"Author"}))
    return(n_authors)   


def count_results(key):  
    """# counts results in pubmed using given key  
    # key - pubmed query"""
    max_results = 1
    sleep(0.3)
    req = requests.get(f"""http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmode=xml&retmax={max_results}&sort=relevance&term={key}""")
    answer = BeautifulSoup(req.text, 'html.parser')
    result = int(answer.find_all("count")[0].get_text())
    return(result)


def get_pub_date(soup):
    """returns publication date
    # soup:
    # link = f'''http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&retmode=xml&id={pmid}'''
    # art = requests.get(link)
    # soup = BeautifulSoup(art.text, 'html.parser') """ 
    date = soup.find_all(attrs={"name": "pubmed","type": "Date"})[0].get_text()
    return(date)


def try_or(func, default=None, expected_exc=(Exception,)):
# allows to write try-except in a single line  
# func - lambda function
    try:
        return func()
    except expected_exc:
        return default

def extract_values(obj, key):
    """Pull all values of specified key from nested JSON."""
    arr = []

    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    results = extract(obj, arr, key)
    return results


def get_cit_by_pmid(pmid=None, apikey="82bd3c40b84225a5981daff35e1c2097"):    #!!!!!!! find out why None for 24649328,24596747,24551446,12725084,
    """retrieves citations count by pmid
    api key is provided by https://dev.elsevier.com/apikey/manage after registration"""
    
    url = f"http://api.elsevier.com/content/search/scopus?query=PMID({pmid})&field=citedby-count"
    sleep(0.3)
    r = requests.get(url,headers={"X-ELS-APIKey": "82bd3c40b84225a5981daff35e1c2097"})
    scopus_json = r.json()
    cit_count = extract_values(scopus_json,'citedby-count')   

    if extract_values(scopus_json,"opensearch:totalResults")[0] == '0':
        print("need pmid")
        return(None,None)

    else:     
        if len(cit_count)==0:
            cit_count = 0
        else:
            cit_count = int(cit_count[0])
        return(cit_count,scopus_json)    
    


def get_list_diff(list1, list2):
    """creates list3 containing elements of list1 which are not in list2"""

    list3 = list(np.setdiff1d(list1,list2))
    return(list3)

def get_table_col(colnames="pmid", 
                  condition="",
                  table="test_articles_1", 
                  database="tumba", 
                  user="xui", 
                  password="mmceez", 
                  host="localhost", 
                  port="5433"):
    
    """returns pandas dataframe of values in colnames of table in postgres database"""
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}", echo = False)    
    Query = f"select {colnames} from {table} {condition};"
    result = pd.read_sql(sqlalchemy.text(Query), engine)
    
    return(result)



def randomizer(start, stop, step):
    """retyurns a function that returns random not repeating number from range(start,stop,step) or None if all numbers are used"""
    l = list(range(start, stop,  step))
    random.shuffle(l)
    
    def pop_from_nonempty():
        if len(l)>0:
            a = l.pop()
            return(a)
        else:
            return(None)
        
    return(pop_from_nonempty)    

    
def table_from_pmids(pmid):
    """creates pd.DataFrame with pubmed papers as observations and features as collumns  """
    soup = try_or(lambda: get_soup(pmid))  
    jsoup = get_soup(pmid,retmode="json")
    #print(pmid)
    cit_count, scopus_json  = get_cit_by_pmid(pmid)
    print(f"scopus_json: {scopus_json}")
    result = {
        "pmid": pmid,
        "publ_date": try_or(lambda: get_pub_date(soup)),
        "pi_name": try_or(lambda: get_PI(soup)),
        "full_affil": try_or(lambda: get_aff_countries(pmid,PI=False)), # !!! change the function to retrieve PI and othe affiliations simultaneously
        "pi_affil": try_or(lambda: get_aff_countries(pmid,PI=True)), # !!! change the function to retrieve PI and othe affiliations simultaneously
        "pi_publ_count": try_or(lambda: count_results(f"{get_PI(soup)}[Author - Last]")),
        "journal": try_or(lambda: get_journal(soup)),
        "journal_publ_count": try_or(lambda: count_results(f"{get_journal(soup)}[Journal]")),
        "authors_count": try_or(lambda: get_n_authors(soup)),
        "cit_count": cit_count,
        "summary": try_or(lambda: json.loads(jsoup.text)),
        "scopus_json": scopus_json,
    }

    print(pmid)
    return(result)
    
