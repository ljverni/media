from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import time
from time import perf_counter
from nacion_infinitescroll import scroll_down
import json
import unicodedata

t1_start = perf_counter()  

class Nacion_scraper:
    
    def __init__(self, base_url):
        self.base_url = base_url
        self.url_all_list = []
        self.url_residuals_list = []
        self.url_filtered_list = []
        
    def url_filter(self, url_list):
        for url in url_list:
            match1 = re.search(r"nid\d{7}", url)
            if match1 and url not in self.url_filtered_list: 
                self.url_filtered_list.append(url)
            else:
                self.url_residuals_list.append(url)
        
    def scrap(self):
        self.url_all_list = scroll_down(self.base_url)
        self.url_filter(self.url_all_list)
        
    def url_alert(self):
        if len(self.url_all_list) != (len(self.url_filtered_list) + len(self.url_residuals_list)):
            print("ALERT: URL FILTER CONDITIONS")
        else:
            print("FILTER WORKING NORMALLY")

class Nacion_extractor:
    
    def __init__(self):
        self.article_dict = {}
        self.current_date = pd.to_datetime("today").date()
        self.date_errors = 0
        self.url_drops = 0
        
    def title_normalizer(self, html):
        title = str(html.title.string)
        article_name_norm = str(unicodedata.normalize('NFKD', title).encode('ASCII', 'ignore'))[2:-1]
        name_cleaned = article_name_norm.replace("- LA NACION", "")
        return name_cleaned.lower().replace("-", " ")

    def date_extractor(self, html):
        months_spanish = {"enero": "01", "febrero": "02", "marzo": "03", "abril": "04", "mayo": "05", "junio": "06", "julio": "07", "agosto": "08", "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12"}
        if hasattr(html.find("section", class_= r"fecha"), "text"):
            date_raw = html.find("section", class_= r"fecha").text
            match_date = re.search(r"\d*\s+de\s+[a-zA-Z]+\s+de\s+\d+", date_raw)
            sdate = match_date.group().split("de")
            date = sdate[2] + "-" + months_spanish[sdate[1].strip()] + "-" + sdate[0]
            date_converted = pd.to_datetime(date).date()
            return date_converted
        else:
            print("Date error")
            self.date_errors += 1
            return self.current_date
    
    def article_extractor(self, html):
        article = "" 
        if html.find("article", class_= r"floatFix").findAll("p"):
            raw_article = html.find("article", class_= r"floatFix").findAll("p")
            for paragraph in raw_article:
                pgraph_joined = ""
                pgraph_splitted = paragraph.text.split(" ")
                for words in pgraph_splitted:
                    pgraph_joined += str(words).strip() + " "        
                article += pgraph_joined
            self.article_dict[self.title_normalizer(html)] = article  
            return article
        elif html.findAll("p"):
            raw_article = html.findAll("p")
            for paragraph in raw_article:
                pgraph_joined = ""
                pgraph_splitted = paragraph.text.split(" ")
                for words in pgraph_splitted:
                    pgraph_joined += str(words).strip() + " "        
                article += pgraph_joined
            self.article_dict[self.title_normalizer(html)] = article  
            return article
        else:
            self.article_dict[self.title_normalizer(html)] = "NA"
        
    
    
    def df_values(self, url_list):
        url_counter = len(url_list)
        d_values = {"date": [], "topic": [], "article_name": [], "url": []}
        for url in url_list:
            print(url + "\n" + str(url_counter) + " URLs left to be parsed")
            url_counter -= 1
            time.sleep(2)
            source = requests.get(url)
            html = BeautifulSoup(source.text, "lxml")
            if html.find("article", class_= r"floatFix"):
                d_values["date"].append(self.date_extractor(html))
                d_values["url"].append(str(url))
                d_values["topic"].append(url.split("/")[3])
                d_values["article_name"].append(self.title_normalizer(html))
                self.article_extractor(html)
            else:
                self.url_drops += 1
                continue
        return d_values
        
    def df_generator(self, url_list):
        df = pd.DataFrame(self.df_values(url_list))
        df["media_name"] = "lanacion"
        df["extraction_date"] = self.current_date
        df.reset_index(drop=True, inplace=True)
        return df

#############
#URL SCRAPER#
#############
nacion = Nacion_scraper("https://www.lanacion.com.ar/")
nacion_html = nacion.scrap()
nacion.url_alert()
filtered = nacion.url_filtered_list
residuals = nacion.url_residuals_list

#############################
#DATA EXTRACTION/DF CREATION#
#############################
nacion_frame = Nacion_extractor()
df = nacion_frame.df_generator(nacion.url_filtered_list)

###############
#LOAD ARTICLES#
###############
json_path = f"local_repo\media\lanacion\data_files\lanacion-articles-{nacion_frame.current_date}.json"
with open(json_path, "w") as write_file:
    json.dump(nacion_frame.article_dict, write_file, indent=4)

################
#DATAFRAME LOAD#
################
# df.to_csv(f"local_repo\media\lanacion\data_files\lanacion_master.csv", index=False)
df.to_csv(f"local_repo\media\lanacion\data_files\lanacion_master.csv", mode="a", index=False, header=False)
csv_daily_path = f"local_repo\media\lanacion\data_files\lanacion-{nacion_frame.current_date}.csv"
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
    if df.shape[0] == len(nacion.url_filtered_list) - nacion_frame.url_drops:
        return f"CSV Rows Loaded: {df.shape[0]} - file created succesfully"
    else:
        return f"CSV Rows Loaded: {df.shape[0]} - Missing rows: {(len(nacion.url_filtered_list) - nacion_frame.url_drops) - df.shape[0]} - Load Error"
    
json_status = json_load_check(loaded_df, loaded_json)
csv_status = csv_load_check(loaded_df)

##########
#LOG FILE#
##########
t1_stop = perf_counter() 

extraction_date = f"Extraction date: {pd.to_datetime('today').strftime('%Y-%m-%d %H:%M:%S')}"
url_info = f"Media URL: {nacion.base_url}\nTotal URL Extracted: {len(nacion.url_all_list)}\nURL Filtered: {len(nacion.url_filtered_list)}\nURL Residuals: {len(nacion.url_residuals_list)}\nURL Droped: {nacion_frame.url_drops}\nDate Errors: {nacion_frame.date_errors}"
url_df = f"URL Loaded DF: {str(df.shape[0])}"
time_info = f"Elapsed time: {str(t1_start)}(Start), {str(t1_stop)}(Stop) \nElapsed time during the whole program in seconds: {str(t1_stop-t1_start)}"

log = extraction_date + "\n" + url_info + "\n" + url_df + "\n" + csv_status + "\n" + json_status + "\n" + time_info
print(log)

# with open(f"local_repo\media\lanacion\data_files\lanacionlog.txt", "w") as file:
#     file.write(log)

with open(f"local_repo\media\lanacion\data_files\lanacionlog.txt", "a+") as log_file:
    log_file.write("\n------------------------\n")
    log_file.write(log)
