# -*- coding: utf-8 -*-

"""Code for corporate filings scraper based on txt and html.

Last run: for S&P 500. Jun 10 2019
Last update: Jun 19 2019 5:00pm

Concerns:
Opposed to Hadoop architecture on AWS proposed by Wolfe's paper, we are using local machine to save files.
A big problem of this scraper is the absolute path should be changed to relative path.
Since we are working on a solution to save all the data, this issue would remain open.

@author: Bruce Wu
"""

import os
import re
from time import gmtime, strftime
import multiprocessing
from datetime import datetime, timedelta
import unicodedata
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import requests
import bs4 as bs
import pickle
from lxml import html
from tqdm import tqdm
from EDGAR_utils import *

# Hard code the last scraped time for no overlapping scraping.
# Future work: save to a Config file
LAST_UPDATE_10K = pd.to_datetime('2019-06-10')
LAST_UPDATE_10Q = pd.to_datetime('2019-06-10')
LAST_UPDATE_8K = pd.to_datetime('2019-06-10')

# Get lists of tickers from NASDAQ, NYSE, AMEX
nasdaq_tickers = pd.read_csv(
    'https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download')
nyse_tickers = pd.read_csv(
    'https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download')
amex_tickers = pd.read_csv(
    'https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=amex&render=download')

# Drop irrelevant cols
nasdaq_tickers.drop(labels='Unnamed: 8', axis='columns', inplace=True)
nyse_tickers.drop(labels='Unnamed: 8', axis='columns', inplace=True)
amex_tickers.drop(labels='Unnamed: 8', axis='columns', inplace=True)

# Create full list of tickers/names across all 3 exchanges
tickers = list(set(list(nasdaq_tickers['Symbol']) +
                   list(nyse_tickers['Symbol']) +
                   list(amex_tickers['Symbol'])))


def MapTickerToCik(tickers: list)->dict:
    """
    This function would return a dictionary mapping from ticker to cik

    :param tickers: list, tickers of all public companies trader on NASDAQ, NYSE, AMEX
    :return: cik_dict: dict, dictionary of a mapping from ticker to cik
    """
    url = 'http://www.sec.gov/cgi-bin/browse-edgar?CIK={}&Find=Search&owner=exclude&action=getcompany'
    cik_re = re.compile(r'.*CIK=(\d{10}).*')

    cik_dict = {}
    for ticker in tqdm(tickers):  # Use tqdm lib for progress bar
        results = cik_re.findall(requests.get(url.format(ticker)).text)
        if len(results):
            cik_dict[str(ticker).lower()] = str(results[0])

    return cik_dict

#cik_dict = MapTickerToCik(tickers)
# This program takes up to 1 hr to run. The user of the script should save it if this dict is determined as useful
# with open('cik_dict.pkl', 'wb') as f:
#     pickle.dump(cik_dict, f)
#
# with open('cik_dict.pkl', 'rb') as f:
#     b = pickle.load(f)

# Otherwise, we can just save the dataframe below


# clean up the ticker-CIK mapping to a pd.DataFrame
ticker_cik_df = pd.DataFrame.from_dict(data=cik_dict, orient='index')
ticker_cik_df.reset_index(inplace=True)
ticker_cik_df.columns = ['ticker', 'cik']
ticker_cik_df['cik'] = [str(cik) for cik in ticker_cik_df['cik']]

# ticker_cik_df.to_csv('ticker_cik_df.csv')

# check for unique pairing
print('Number of ticker-cik pairings: ', len(ticker_cik_df))
print('Number of unique tickers: ', len(set(ticker_cik_df['ticker'])))
print('Number of unique CIKs', len(set(ticker_cik_df['cik'])))

'''
Number of ticker-cik pairings:  5062
Number of unique tickers:  5062
Number of unique CIKs 4869
'''
# keep first ticker alphabetically for duplicate CIKS
ticker_cik_df = ticker_cik_df.sort_values(by='ticker')
ticker_cik_df.drop_duplicates(subset='cik', keep='first', inplace=True)
print('Number of ticker-cik pairings: ', len(ticker_cik_df))
print('Number of unique tickers: ', len(set(ticker_cik_df['ticker'])))
print('Number of unique CIKs', len(set(ticker_cik_df['cik'])))


'''
So far, we have a list of the CIKs for which we want to obtain 10-Ks and 10-Qs. We can now begin scraping from EDGAR.
We also probably want to log warnings/errors and save that log, just in case we need to reference it later.
The SEC limits users to 10 requests per second, so we need to make sure we're not making requests too quickly.
'''

ticker_cik_df = pd.read_csv('ticker_cik_df.csv')


def WriteLogFile(log_file_name: str, text: str)->None:
    """
    This function writes a log file with all notes and
    error messages from a scraping "session".

    :param log_file_name: str, Name of the log file (should be a .txt file).
    :param text: str, Text to write to the log file.
    :return: None
    """
    with open(log_file_name, "a") as log_file:
        log_file.write(text)

    return

# ----------------------------------------------------------------------------------------------


def filing_scraper(
        browse_url_base: str,
        filing_url_base: str,
        doc_url_base: str,
        cik: str,
        log_file_name: str,
        file_type: str,
        first_time_scraping: bool)->None:
    """
    Scrapes all files for a particular file type for a particular CIK from EDGAR.
    Note that 10-Ks include 10-Ks and 10-K405s

    :param browse_url_base: str, Base URL for browsing EDGAR.
    :param filing_url_base: str, Base URL for filings listings on EDGAR.
    :param doc_url_base: str, Base URL for one filing's document tables page on EDGAR.
    :param cik: str, Central Index Key.
    :param log_file_name: str, Name of the log file (should be a .txt file).
    :param file_type: str, Type of file we want to scrap (e.g 10-K, 10-Q, 8-K)
    :param first_time_scraping: bool, If True then we are scraping for the first time. Otherwise we are only updating
    :return: None
    """

    cik = str(cik)
    # this class currently supports 3 types of files.
    assert file_type in ['10-K', '10-Q', '8-K']

    if first_time_scraping:
        try:
            os.mkdir(cik)
        except OSError:
            print('CIK already scraped', cik)

    print('scraping CIK', cik)
    os.chdir(cik)

    # Request list of 10-K filings
    res = requests.get(browse_url_base % cik)

    # If the request failed, log the failure and exit
    if res.status_code != 200:
        os.chdir('..')
        # os.rmdir(cik) # remove empty dir
        text = "Request failed with error code " + str(res.status_code) + \
               "\nFailed URL: " + (browse_url_base % cik) + '\n'
        WriteLogFile(log_file_name, text)
        return

    # If the request doesn't fail, continue...

    # parse the response HTML using BeautifulSoup
    soup = bs.BeautifulSoup(res.text, 'lxml')

    # extract all tables from the response
    html_tables = soup.find_all('table')

    # Check that the table we're looking for exists
    # If it doesn't, exit
    if len(html_tables) < 3:
        os.chdir('..')
        return

    filings_table = pd.read_html(str(html_tables[2]), header=0)[0]

    filings_table['Filings'] = [str(x) for x in filings_table['Filings']]

    if not first_time_scraping:
        if (file_type == '10-K') and (pd.to_datetime(
                filings_table['Filing Date'][0]) > LAST_UPDATE_10K):
            filings_table = filings_table[(
                filings_table['Filings'] == '10-K') | (filings_table['Filings'] == '10-K405')]
        elif (file_type == '10-Q') and (pd.to_datetime(filings_table['Filing Date'][0]) > LAST_UPDATE_10Q):
            filings_table = filings_table[filings_table['Filings'] == '10-Q']
        elif (file_type == '8-K') and (pd.to_datetime(filings_table['Filing Date'][0]) > LAST_UPDATE_8K):
            filings_table = filings_table[filings_table['Filings'] == '8-K']
        else:
            print(
                f'no new filing for the ticker to download as {datetime.now()}')
            pass
    else:
        if (file_type ==
                '10-K'):
            filings_table = filings_table[(
                filings_table['Filings'] == '10-K') | (filings_table['Filings'] == '10-K405')]
        elif (file_type == '10-Q'):
            filings_table = filings_table[filings_table['Filings'] == '10-Q']
        elif (file_type == '8-K'):
            filings_table = filings_table[filings_table['Filings'] == '8-K']
        else:
            print(
                f'no new filing for the ticker to download as {datetime.now()}')
            pass

    # if Filings table does not have any 10-K or 10-K405s
    if len(filings_table) == 0:
        os.chdir('..')
        return

    # get accession number for each 10-K and 10-K405 filing
    filings_table['Acc_No'] = [x.replace('\xa0', ' ') .split(
        'Acc-no: ')[1] .split(' ')[0] for x in filings_table['Description']]

    # iterate through each filing and scrape corresponding document

    for index, row in filings_table.iterrows():

        # get the accession number for the filing
        acc_no = str(row['Acc_No'])

        # navigate to the page for the filing
        docs_page = requests.get(filing_url_base % (cik, acc_no))

        # if request fails, log the failure and skip to next filing
        if docs_page.status_code != 200:
            os.chdir('..')
            text = 'Request failed with error code ' + str(docs_page.status_code) + \
                '\nFailed URL: ' + (filing_url_base % (cik, acc_no)) + '\n'
            WriteLogFile(log_file_name, text)
            os.chdir(cik)
            continue

        # if request succeeds

        # parse the table of documents for the filing
        docs_page_soup = bs.BeautifulSoup(docs_page.text, 'lxml')
        docs_html_tables = docs_page_soup.find_all('table')
        if len(docs_html_tables) == 0:
            continue
        docs_table = pd.read_html(str(docs_html_tables[0]), header=0)[0]
        docs_table['Type'] = [str(x) for x in docs_table['Type']]

        if file_type == '10-K':
            docs_table[(docs_table['Type'] == '10-K') |
                       (docs_table['Type'] == '10-K405')]
        elif file_type == '10-Q':
            docs_table = docs_table[docs_table['Type'] == '10-Q']
        elif file_type == '8-K':
            docs_table = docs_table[docs_table['Type'] == '8-K']
        # if there are not corresponding filing, skip to next filing

        if len(docs_table) == 0:
            print(
                'no corresponding filing, could be due to the newest filing is already in the database')
            print('Please check file date for this CIK')
            print('Preceeding to next filing')
            continue

        elif len(docs_table) > 0:
            docs_table = docs_table.iloc[0]

        docname = docs_table['Document']

        # if the first entry is unavailable, log the failure and exit

        if str(docname) == 'nan':

            os.chdir('..')
            text = 'File with CIK: %s and Acc_No: %s is unavailable' % (
                cik, acc_no) + '\n'
            WriteLogFile(log_file_name, text)
            os.chdir(cik)
            continue

        # if it is available
        file = requests.get(doc_url_base %
                            (cik, acc_no.replace('-', ''), docname))

        # If the request fails, log the failure and exit
        if file.status_code != 200:
            os.chdir('..')
            text = "Request failed with error code " + str(file.status_code) + "\nFailed URL: " + (
                doc_url_base % (cik, acc_no.replace('-', ''), docname)) + '\n'
            WriteLogFile(log_file_name, text)
            os.chdir(cik)
            continue

        # if it succeeds
        if '.txt' in docname:
            date = str(row['Filing Date'])
            filename = cik + '_' + date + '.txt'
            html_file = open(filename, 'a')
            html_file.write(file.text)
            html_file.close()
        else:
            # Save text as HTML
            date = str(row['Filing Date'])
            filename = cik + '_' + date + '.html'
            html_file = open(filename, 'a')
            html_file.write(file.text)
            html_file.close()

    # Move back to the main filings directory
    os.chdir('..')

    return


# ----------------------------------------------------------------------------------------------

def run_scraper(
        df: pd.DataFrame,
        file_type: str,
        chunk_size: int,
        first_time_flag: bool)-> None:
    """
    The function actually runs the scraper for each file type filings.

    :param df: pd.DataFrame, the dataframe which contains all ciks
    :param file_type: str, file type we want to scrape
    :param chunk_size: int, how many ticker we want to scrape for one run
    :param first_time_flag: bool, whether we are scraping for the first time or updating
    :return: None
    """
    # will move to config.py eventually
    pathname = 'C:/Users/bwu/PycharmProjects/EDGAR/data/' + \
        str(file_type) + 's'
    assert file_type in ['10-K', '10-Q', '8-K']

    filing_url_base = 'http://www.sec.gov/Archives/edgar/data/%s/%s-index.html'
    doc_url_base = 'http://www.sec.gov/Archives/edgar/data/%s/%s/%s'

    # to find the index of K or Q
    bool_list = [_ in ['K', 'Q'] for _ in file_type]
    letter_index = [i for i, x in enumerate(bool_list) if x][0]
    browse_url_base = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=%s&type=' + \
        file_type[:letter_index - 1] + '-' + file_type[letter_index]

    os.chdir(pathname)
    # Initialize log file
    # (log file name = the time we initiate scraping session)
    time = strftime("%Y-%m-%d %Hh%Mm%Ss", gmtime())
    log_file_name = 'log ' + time + '.txt'
    with open(log_file_name, 'a') as log_file:
        log_file.close()

    for cik in tqdm(df['cik'][:chunk_size]):
        filing_scraper(browse_url_base=browse_url_base,
                       filing_url_base=filing_url_base,
                       doc_url_base=doc_url_base,
                       cik=cik,
                       log_file_name=log_file_name,
                       file_type=file_type,
                       first_time_scraping=first_time_flag)

# ---------------------scraper execution-------------------------------------
# full data downloading for all 4869 tickers
#run_scraper(ticker_cik_df,'10-K', len(tikcer_cik_df), True)
#run_scraper(ticker_cik_df,'10-Q', len(tikcer_cik_df), True)
#run_scraper(ticker_cik_df,'8-K', len(tikcer_cik_df), True)


# get s&p 500 ciks
wiki_url_sp500 = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
cik_df_sp500 = pd.read_html(wiki_url_sp500, header=[0], index_col=0)[0]

# use s&p 500 as test batch

run_scraper(ticker_cik_df_sp500, '10-K', len(ticker_cik_df_sp500), False)
run_scraper(ticker_cik_df_sp500, '10-Q', len(ticker_cik_df_sp500), False)
run_scraper(ticker_cik_df_sp500, '8-K', len(ticker_cik_df_sp500), False)
