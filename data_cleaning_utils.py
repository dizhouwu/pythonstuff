from nltk.corpus import wordnet
from nltk.stem import WordNetLemmatizer
import nltk
import numpy as np
import pandas as pd
import multiprocessing
import pickle
import pprint
from tqdm import tqdm
import re
import os
from shutil import rmtree
from time import gmtime, strftime
from datetime import datetime, timedelta
import unicodedata
from sklearn.metrics.pairwise import cosine_similarity
import requests
import functools
from nltk import word_tokenize
from nltk.tokenize import RegexpTokenizer
from nltk.stem import WordNetLemmatizer, SnowballStemmer
from nltk.corpus import stopwords
from bs4 import BeautifulSoup
from lxml import html
from edgar_utils_refactor import *

# nltk packages
# nltk.download('stopwords')
# nltk.download('wordnet')
#nltk.download('punkt')

sentiment_df = master_dictionary_df.copy(deep=True)

def remove_numerical_tables(soup: BeautifulSoup, percent: float)->BeautifulSoup:
    """
    Removes tables with percent% numerical characters.

    :param soup: BeautifulSoup object. Parsed result from BeautifulSoup.
    :param percent: Percentage of numerical characters to remove
    :return: soup : BeautifulSoup object, Parsed result from BeautifulSoup with numerical tables removed.
    """

    def get_numeric_percent(tablestring: str)->float:
        """
        Calculate Percentage of numerical characters

        :param tablestring: str, strings in the table
        :return: percentage: float
        """
        if len(tablestring) > 0.0:
            numbers = sum([char.isdigit() for char in tablestring])
            length = len(tablestring)
            percentage = numbers / length
            return percentage
        else:
            return 1.0

    # returns the tag or string that was extracted:
    [x.extract() for x in soup.find_all('table')
     if get_numeric_percent(x.get_text()) > percent]

    return soup


def remove_tags(soup: BeautifulSoup)->str:
    """
    Drops HTML tags, newlines and unicode text from
    filing text.

    :param soup: BeautifulSoup object. Parsed result from BeautifulSoup.
    :return: text : str, Filing text.
    """

    text = soup.get_text()

    # remove newline character
    text = text.replace('\n', ' ')

    # normalize unicode characters
    text = unicodedata.normalize('NFKD', text)

    return text

def convert_html(cik: str)->None:
    """
    Removes numerical tables, HTML tags,
    newlines, unicode text, and XBRL tables.

    :param cik: str, Central Index Key used to scrape files.
    :return: None.
    """

    cik = str(cik)
    try:
        os.chdir(cik)
    except FileNotFoundError:
        print(f'cannot find directory of {cik}')
        return

    print("Cleaning CIK %s..." % cik)

    parsed = False  # parse flag
    # make a new directory within the CIK directory
    try:
        os.mkdir('rawtext')
    # If it already exists, continue
    # We can't exit at this point because we might be
    # partially through parsing text files, so we need to continue
    except OSError:
        pass

    # Get list of scraped files excluding hidden files and directories
    file_list = [fname for fname in os.listdir() if not (
        fname.startswith('.') | os.path.isdir(fname))]

    for filename in file_list:
        # check if the file has already been cleaned
        new_filename = filename.replace('.html', '.txt')
        text_file_list = os.listdir('rawtext')
        if new_filename in text_file_list:
            continue

        # keep cleaning files if it has not been cleaned
        with open(filename, 'r') as file:
            parsed = True
            soup = BeautifulSoup(file.read(), 'lxml')
            soup = remove_numerical_tables(soup, 0.15) # 0.15 is suggested in the Lazy Price paper
            text = remove_tags(soup)
            with open('rawtext/' + new_filename, 'w', encoding='utf-8') as newfile:
                newfile.write(text)

    # if all files in the cik directory have been parsed
    if not parsed:
        print('Already parsed CIK', cik)

    os.chdir('..')
    return

def clean_filings(file_type: str, chunksize: int)->None:
    """
    Perform the cleaning

    :param file_type: str, e.g '10-K', '10-Q', '8-K'
    :param chunksize: str, how many we want to clean.
    :return: None
    """
    assert file_type in ['10-K', '10-Q', '8-K']
    pathname = get_pathname_file_type(file_type)
    os.chdir(pathname)
    for cik in tqdm(cik_secid_mapping['cik'][:chunksize]):
        convert_html(cik)

def clean_text(text: str, lm_stop_words: bool = True, drop_lm_word: bool = False)->list:
    """
    Clean a text string

    :param txt: the text string to be cleaned
    :param lm_stop_words: bool, whether using StopWords_Generic.txt provided by LM
    :return:
    """

    wnl = WordNetLemmatizer()

    def tokenize(text):
        # normalize case and remove punctuation
        text = re.sub(r"[^a-zA-Z0-9]", " ", text.lower())

        # tokenize text
        tokens = word_tokenize(text)

        # lemmatize andremove stop words
        tokens = [wnl.lemmatize(word) for word in tokens if word not in stop_words]

        return tokens

    if lm_stop_words:
        stop_words = ()
        with open(SOURCE_PATH+'/data/StopWords_Generic.txt','r', encoding='utf-8') as f:
            for line in f:
                line = line.lower().strip('\n')
                stop_words += (line,)
    else: # using nltk stopwords with some added stopwords

        stop_words = set(stopwords.words('english'))
        # add some extra stopwords
        stop_words |= {
            "may",
            "business",
            "company",
            "could",
            "service",
            "result",
            "product",
            "operation",
            "include",
            "law",
            "tax",
            "change",
            "financial",
            "require",
            "cost",
            "market",
            "also",
            "user",
            "plan",
            "actual",
            "cash",
            "other",
            "thereto",
            "thereof",
            "therefore"}

    cleaned_txt = tokenize(text)

    if drop_lm_word:
        cleaned_txt = ','.join(word for word in cleaned_txt if word in set(sentiment_df['word']))
    else:
        pass

    return cleaned_txt

