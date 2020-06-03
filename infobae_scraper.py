from selenium import webdriver
from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import time
from time import perf_counter
import json
import unicodedata

t1_start = perf_counter()  

class Infobae_scraper:
    
    def __init__(self, base_url):
        self.base_url = base_url
        self.url_all_list = []
        self.url_residuals_list = []
        self.url_filtered_list = []

    def get_html(self, url):
        source = requests.get(url)
        html = BeautifulSoup(source.text, "lxml")
        return html
    
    def get_url(self, html):
        link_anchors = html.find_all("a")
        for anchors in link_anchors:
            url = anchors.get("href")
            if type(url) == str:
                self.url_all_list.append(url)
        
    def url_filter(self, url_list):
        for url in url_list:
            match1 = re.search(r"\d{4}.\d{2}.\d{2}", url)
            match2 = re.search(r"https", url)
            match3 = re.search(r"/fotos/", url)
            if match1 and not match2 and not match3:
                url = "https://www.infobae.com" + url
                if url not in self.url_filtered_list:
                    self.url_filtered_list.append(url)
                else:
                    self.url_residuals_list.append(url)
            else:
                self.url_residuals_list.append(url)
        
    def scrap(self):
        html = self.get_html(self.base_url)
        self.get_url(html)
        self.url_filter(self.url_all_list)
        
    def url_alert(self):
        if len(self.url_all_list) != (len(self.url_filtered_list) + len(self.url_residuals_list)):
            print("ALERT: URL FILTER CONDITIONS")
        else:
            print("FILTER WORKING NORMALLY")

class Infobae_extractor:
    
    def __init__(self):
        self.article_dict = {}
        self.current_date = pd.to_datetime("today").date()
        self.date_errors = 0
        self.url_drops = 0
        
    def title_normalizer(self, html):
        title = str(html.title.string)
        article_name_norm = str(unicodedata.normalize('NFKD', title).encode('ASCII', 'ignore'))[2:-1]
        return article_name_norm.lower().replace("- infobae", "").replace("'", "")

    def date_extractor(self, url):
        s_url = url.split("/")
        for n in range(len(s_url)):
            match = re.search(r"\d{4}", s_url[n])
            if match:
                date = s_url[n] + "-" + s_url[n+1] + "-" + s_url[n+2]
                date_converted = pd.to_datetime(date).date()
                return date_converted
        
    def topic_extractor(self, url):
        splitted_url = url.split("/")
        for n in range(len(splitted_url)):
            match = re.search(r"\d{4}", splitted_url[n])
            if match:
                return splitted_url[n-1]
    
    def article_extractor(self, html):
        article = ""
        if html.findAll("p", class_= r"font_tertiary paragraph"):
            raw_article = html.findAll("p", class_= r"font_tertiary paragraph")
            for paragraphs in raw_article:
                norm_paragraph = str(unicodedata.normalize('NFKD', paragraphs.text).encode('ASCII', 'ignore'))[2:] + "\n"
                article += norm_paragraph
                self.article_dict[self.title_normalizer(html)] = article.replace("'", "")
            return article
        elif html.findAll("p", class_="element element-paragraph"):
            raw_article = html.findAll("p", class_="element element-paragraph")
            for paragraphs in raw_article:
                norm_paragraph = str(unicodedata.normalize('NFKD', paragraphs.text).encode('ASCII', 'ignore'))[2:] + "\n"
                article += norm_paragraph
                self.article_dict[self.title_normalizer(html)] = article.replace("'", "")
            return article
        elif html.findAll("p"):
            raw_article = html.findAll("p")
            for paragraphs in raw_article:
                norm_paragraph = str(unicodedata.normalize('NFKD', paragraphs.text).encode('ASCII', 'ignore'))[2:] + "\n"
                article += norm_paragraph
                self.article_dict[self.title_normalizer(html)] = article.replace("'", "")
            return article
        else:
            self.article_dict[self.title_normalizer(html)] = "NA"
    
    def df_values(self, url_list):
        url_counter = len(url_list) - 1
        d_values = {"date": [], "topic": [], "article_name": [], "url": []}
        for url in url_list:
            print(url + "\n" + str(url_counter) + " URLs left to be parsed")
            url_counter -= 1
            time.sleep(2)
            html = infobae.get_html(url)
            d_values["date"].append(self.date_extractor(url))
            d_values["url"].append(str(url))
            d_values["topic"].append(self.topic_extractor(url))
            d_values["article_name"].append(self.title_normalizer(html))
            self.article_extractor(html) 
        return d_values
    
    def df_generator(self, url_list):
        df = pd.DataFrame(self.df_values(url_list))
        df["media_name"] = "infobae" 
        df["extraction_date"] = self.current_date
        df.reset_index(drop=True, inplace=True)
        return df


#############
#URL SCRAPER#
#############
infobae = Infobae_scraper("https://www.infobae.com/?noredirect")
infobae_html = infobae.scrap()
infobae.url_alert()


#############################
#DATA EXTRACTION/DF CREATION#
#############################
infobae_frame = Infobae_extractor()
df = infobae_frame.df_generator(infobae.url_filtered_list)

###############
#LOAD ARTICLES#
###############
json_path = f"local_repo\media\infobae\data_files\infobae-articles-{infobae_frame.current_date}.json"
with open(json_path, "w") as write_file:
    json.dump(infobae_frame.article_dict, write_file, indent=4)

################
#DATAFRAME LOAD#
################
# df.to_csv(f"local_repo\media\infobae\data_files\infobae_master.csv", index=False)
df.to_csv(f"local_repo\media\infobae\data_files\infobae_master.csv", mode="a", index=False, header=False)
csv_daily_path = f"local_repo\media\infobae\data_files\infobae-{infobae_frame.current_date}.csv"
df.to_csv(csv_daily_path, index=False)

############
#LOAD CHECK#
############
loaded_df = pd.read_csv(csv_daily_path)

with open(json_path, "r") as json_file:
    loaded_json = json.load(json_file)

def json_load_check(df, file_data):
    counter = 0
    for index, row in df.iterrows():
        if file_data[row[2]]:
            counter += 1
        else:
            continue
    na_counter = 0
    for key in file_data:
        if file_data[key] == "NA":
            na_counter += 1
    if counter == df.shape[0]:
        return f"JSON Total Values({counter}): {counter - na_counter} Articles Loaded, {na_counter} NA Values Loaded - file created succesfully"
    else:
        return f"JSON Total Values({counter}): {counter - na_counter} Articles Loaded, {na_counter} NA Values Loaded - WARNING file incomplete"
        
def csv_load_check(df):
    if df.shape[0] == len(infobae.url_filtered_list) - infobae_frame.url_drops:
        return f"CSV Rows Loaded: {df.shape[0]} - file created succesfully"
    else:
        return f"CSV Rows Loaded: {df.shape[0]} - Missing rows: {(len(infobae.url_filtered_list) - infobae_frame.url_drops) - df.shape[0]} - Load Error"
    
json_status = json_load_check(loaded_df, loaded_json)
csv_status = csv_load_check(loaded_df)

##########
#LOG FILE#
##########
t1_stop = perf_counter() 

extraction_date = f"Extraction date: {pd.to_datetime('today').strftime('%Y-%m-%d %H:%M:%S')}"
url_info = f"Media URL: {infobae.base_url}\nTotal URL Extracted: {len(infobae.url_all_list)}\nURL Filtered: {len(infobae.url_filtered_list)}\nURL Residuals: {len(infobae.url_residuals_list)}\nURL Droped: {infobae_frame.url_drops}\nDate Errors: {infobae_frame.date_errors}"
url_df = f"URL Loaded DF: {str(df.shape[0])}"
time_info = f"Elapsed time: {str(t1_start)}(Start), {str(t1_stop)}(Stop) \nElapsed time during the whole program in seconds: {str(t1_stop-t1_start)}"

log = extraction_date + "\n" + url_info + "\n" + url_df + "\n" + csv_status + "\n" + json_status + "\n" + time_info
print(log)


# with open(f"local_repo\media\infobae\data_files\infobaelog.txt", "w") as file:
#     file.write(log)

with open(f"local_repo\media\infobae\data_files\infobaelog.txt", "a+") as log_file:
    log_file.write("\n------------------------\n")
    log_file.write(log)
