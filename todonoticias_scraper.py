from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import time
from time import perf_counter
import json
import unicodedata

t1_start = perf_counter()  

class TN_scraper:
    
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
            match1 = re.search(r"\A\/[a-zA-z0-9]+.+\d{7}", url)
            match2 = re.search(r"/videos/", url)
            if match1 and not match2 and url not in self.url_filtered_list:
                self.url_filtered_list.append(url)
            else:
                self.url_residuals_list.append(url)
        self.url_filtered_list = ["https://tn.com.ar" + url for url in self.url_filtered_list]
        
    def scrap(self):
        html = self.get_html(self.base_url)
        self.get_url(html)
        self.url_filter(self.url_all_list)
        
    def url_alert(self):
        if len(self.url_all_list) != (len(self.url_filtered_list) + len(self.url_residuals_list)):
            print("ALERT: URL FILTER CONDITIONS")
        else:
            print("FILTER WORKING NORMALLY")

class TN_extractor:
    def __init__(self):
        self.article_dict = {}
        self.current_date = pd.to_datetime("today").date()
        self.date_errors = 0
        self.url_drops = 0
        
    def title_normalizer(self, html):
        title = html.find("h1", class_="article__title").text
        article_name_norm = str(unicodedata.normalize('NFKD', title).encode('ASCII', 'ignore'))[1:]
        article_cleaned = article_name_norm.replace("| TN'", "")
        return article_cleaned.lower().replace("'", "")

    def date_extractor(self, html):
        raw_date = html.find("div", class_="article__name__and__date").text
        match_date = re.search(r"\d{2}.\d{2}.\d{4}", raw_date)
        sdate = match_date.group().split(("/"))
        fixed_date = pd.to_datetime("2020" + "-" + sdate[1] + "-" + sdate[0]).date()
        return fixed_date
    
    def article_extractor(self, html):
        article = "" 
        if html.find("div", class_= r"article__content").findAll("p"):
            raw_article = html.find("div", class_= r"article__content").findAll("p")
            for paragraph in raw_article:
                normal_pgraph = str(unicodedata.normalize('NFKD', paragraph.text).encode('ASCII', 'ignore'))
                article += normal_pgraph[2:] + "\n"
            self.article_dict[self.title_normalizer(html)] = article
            return article
        elif html.findAll("p"):
            raw_article = html.findAll("p")
            for paragraph in raw_article:
                normal_pgraph = str(unicodedata.normalize('NFKD', paragraph.text).encode('ASCII', 'ignore'))
                article += normal_pgraph[2:] + "\n"
            self.article_dict[self.title_normalizer(html)] = article
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
            html = tn.get_html(url)
            d_values["date"].append(self.date_extractor(html))
            d_values["url"].append(str(url))
            d_values["topic"].append(url.split("/")[3])
            d_values["article_name"].append(self.title_normalizer(html))
            self.article_extractor(html) 
        return d_values
        
    def df_generator(self, url_list):
        df = pd.DataFrame(self.df_values(url_list))
        df["media_name"] = "todonoticias"
        df["extraction_date"] = self.current_date
        df.reset_index(drop=True, inplace=True)
        return df


#############
#URL SCRAPER#
#############
tn = TN_scraper("https://tn.com.ar/")
tn_html = tn.scrap()
tn.url_alert()



#############################
#DATA EXTRACTION/DF CREATION#
#############################
tn_frame = TN_extractor()
df = tn_frame.df_generator(tn.url_filtered_list)


##############
#ARTICLE LOAD#
##############
json_path = fr"local_repo\media\todonoticias\data_files\todonoticias-articles-{tn_frame.current_date}.json"
with open(json_path, "w") as write_file:
    json.dump(tn_frame.article_dict, write_file, indent=4)

################
#DATAFRAME LOAD#
################
# df.to_csv(fr"local_repo\media\todonoticias\data_files\todonoticias_master.csv", index=False)
df.to_csv(fr"local_repo\media\todonoticias\data_files\todonoticias_master.csv", mode="a", index=False, header=False)
csv_daily_path = fr"local_repo\media\todonoticias\data_files\todonoticias-{tn_frame.current_date}.csv"
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
        return f"JSON Values({counter}): {counter - na_counter} Articles Loaded, {na_counter} NA Values Loaded - file created succesfully"
    else:
        return f"JSON Values({counter}): {counter - na_counter} Articles Loaded, {na_counter} NA Values Loaded - WARNING file incomplete"
        
def csv_load_check(df):
    if df.shape[0] == len(tn.url_filtered_list) - tn_frame.url_drops:
        return f"CSV Rows Loaded: {df.shape[0]} - file created succesfully"
    else:
        return f"CSV Rows Loaded: {df.shape[0]} - Missing rows: {(len(tn.url_filtered_list) - tn_frame.url_drops) - df.shape[0]} - Load Error"
    
json_status = json_load_check(loaded_df, loaded_json)
csv_status = csv_load_check(loaded_df)

##########
#LOG FILE#
##########

t1_stop = perf_counter() 

extraction_date = f"Extraction date: {pd.to_datetime('today').strftime('%Y-%m-%d %H:%M:%S')}"
url_info = f"Media URL: {tn.base_url}\nTotal URL Extracted: {len(tn.url_all_list)}\nURL Filtered: {len(tn.url_filtered_list)}\nURL Residuals: {len(tn.url_residuals_list)}\nURL Droped: {tn_frame.url_drops}\nDate Errors: {tn_frame.date_errors}"
url_df = f"URL Loaded DF: {str(df.shape[0])}"
time_info = f"Elapsed time: {str(t1_start)}(Start), {str(t1_stop)}(Stop) \nElapsed time during the whole program in seconds: {str(t1_stop-t1_start)}"


log = extraction_date + "\n" + url_info + "\n" + url_df + "\n" + csv_status + "\n" + json_status + "\n" + time_info
print(log)


# with open(r"local_repo\media\todonoticias\data_files\todonoticiaslog.txt", "w") as file:
#     file.write(log)

with open(fr"local_repo\media\todonoticias\data_files\todonoticiaslog.txt", "a+") as log_file:
    log_file.write("\n------------------------\n")
    log_file.write(log)
