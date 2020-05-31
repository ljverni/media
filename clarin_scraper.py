from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import time
from time import perf_counter
from clarin_infinitescroll import scroll_down
import json
import unicodedata

t1_start = perf_counter()

class Clarin_scraper:
    
    def __init__(self, base_url):
        self.base_url = base_url
        self.url_all_list = []
        self.url_residuals_list = []
        self.url_filtered_list = []
        
    def url_filter(self, url_list):
        for url in url_list:
            match1 = re.search(r"www.clarin.com", url)
            match2 = re.search(r"html$", url)
            if match1 and match2 and len(url.split("/")) >= 5 and url not in self.url_filtered_list and "fotogaleria" not in url:
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

class Clarin_extractor:
    
    def __init__(self):
        self.article_dict = {}
        self.current_date = pd.to_datetime("today").date()
        self.date_errors = 0
        self.url_drops = 0
        
    def date_extractor(self, html):
        if hasattr(html.find("div", class_= r"breadcrumb"), "span"):
            raw_date = str(html.find("div", class_= r"breadcrumb").span.text)
            match_date = re.search(r"\d\d\/\d\d\/\d\d\d\d", raw_date)
            s_date = match_date.group().split("/")
            j_date = "2020" + "-" + s_date[1] + "-" + s_date[0]
            fixed_date = pd.to_datetime(j_date).date()
            return fixed_date
        else:
            self.date_errors += 1
            print("Date Error")
            return self.current_date
    
    def title_normalizer(self, html):
        title = str(html.title.string)
        article_name_norm = str(unicodedata.normalize('NFKD', title).encode('ASCII', 'ignore'))[2:-1]
        name_cleaned = article_name_norm.lower().replace("- clarin", "")
        return name_cleaned.replace("-", " ")
    
    def article_extractor(self, html):
        article = "" 
        if html.find("div", class_="body-nota").findAll("p"):
            raw_article = html.find("div", class_="body-nota").findAll("p")
            for paragraph in raw_article[:-6]:
                article += paragraph.text + "\n"
            self.article_dict[self.title_normalizer(html)] = article
            return article
        elif html.findAll("p"):
            raw_article = html.findAll("p")
            for paragraph in raw_article:
                article += paragraph.text + "\n"
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
            source = requests.get(url)
            html = BeautifulSoup(source.text, "lxml")
            if hasattr(html.find("div", class_="body-nota"), "findAll"):
                d_values["date"].append(self.date_extractor(html))
                d_values["url"].append(str(url))
                d_values["article_name"].append(self.title_normalizer(html))
                d_values["topic"].append(str(url.split("/")[3]))
                self.article_extractor(html)
            else:
                print("URL dropped")
                self.url_drops += 1
                continue
        return d_values
        
    def df_generator(self, url_list):
        df = pd.DataFrame(self.df_values(url_list))
        df["media_name"] = "clarin" 
        df["extraction_date"] = self.current_date
        df.reset_index(drop=True, inplace=True)
        return df


#############
#URL SCRAPER#
#############
clarin = Clarin_scraper("https://www.clarin.com/")
clarin_html = clarin.scrap()
clarin.url_alert()

#############################
#DATA EXTRACTION/DF CREATION#
#############################
clarin_frame = Clarin_extractor()
df = clarin_frame.df_generator(clarin.url_filtered_list)


##############
#ARTICLE LOAD#
##############
json_path = f"local_repo\media\clarin\data_files\clarin-articles-{clarin_frame.current_date}.json"
with open(json_path, "w") as write_file:
    json.dump(clarin_frame.article_dict, write_file, indent=4)

################
#DATAFRAME LOAD#
################
# df.to_csv(f"local_repo\media\clarin\data_files\clarin_master.csv", index=False)
df.to_csv(f"local_repo\media\clarin\data_files\clarin_master.csv", mode="a", index=False, header=False)
csv_daily_path = f"local_repo\media\clarin\data_files\clarin-{clarin_frame.current_date}.csv"
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
    if df.shape[0] == len(clarin.url_filtered_list) - clarin_frame.url_drops:
        return f"CSV Loaded: {df.shape[0]} - file created succesfully"
    else:
        return f"CSV Rows Loaded: {df.shape[0]} - Missing rows: {(len(clarin.url_filtered_list) - clarin_frame.url_drops) - df.shape[0]} - Load Error"
    
json_status = json_load_check(loaded_df, loaded_json)
csv_status = csv_load_check(loaded_df)

##########
#LOG FILE#
##########
t1_stop = perf_counter() 

extraction_date = f"Extraction date: {pd.to_datetime('today').strftime('%Y-%m-%d %H:%M:%S')}"
url_info = f"Media URL: {clarin.base_url}\nTotal URL Extracted: {len(clarin.url_all_list)}\nURL Filtered: {len(clarin.url_filtered_list)}\nURL Residuals: {len(clarin.url_residuals_list)}\nURL Droped: {clarin_frame.url_drops}\nDate Errors: {clarin_frame.date_errors}"
url_df = f"URL Loaded DF: {str(df.shape[0])}"
time_info = f"Elapsed time: {str(t1_start)}(Start), {str(t1_stop)}(Stop) \nElapsed time during the whole program in seconds: {str(t1_stop-t1_start)}"

log = extraction_date + "\n" + url_info + "\n" + url_df + "\n" +  csv_status + "\n" + json_status + "\n" + time_info
print(log)


# with open(f"local_repo\media\clarin\data_files\clarinlog.txt", "w") as file:
#     file.write(log)

with open(f"local_repo\media\clarin\data_files\clarinlog.txt", "a+") as log_file:
    log_file.write("\n------------------------\n")
=======
from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import time
from time import perf_counter
from infinitescroll import scroll_down
import json
import unicodedata

t1_start = perf_counter()

class Clarin_scraper:
    
    def __init__(self, base_url):
        self.base_url = base_url
        self.url_all_list = []
        self.url_residuals_list = []
        self.url_filtered_list = []
        
    def url_filter(self, url_list):
        for url in url_list:
            match1 = re.search(r"www.clarin.com", url)
            match2 = re.search(r"html$", url)
            if match1 and match2 and len(url.split("/")) >= 5 and url not in self.url_filtered_list and "fotogaleria" not in url:
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

class Clarin_extractor:
    
    def __init__(self):
        self.article_dict = {}
        self.current_date = pd.to_datetime("today").date()
        self.date_errors = 0
        self.url_drops = 0
        
    def date_extractor(self, html):
        if hasattr(html.find("div", class_= r"breadcrumb"), "span"):
            raw_date = str(html.find("div", class_= r"breadcrumb").span.text)
            match_date = re.search(r"\d\d\/\d\d\/\d\d\d\d", raw_date)
            s_date = match_date.group().split("/")
            j_date = "2020" + "-" + s_date[1] + "-" + s_date[0]
            fixed_date = pd.to_datetime(j_date).date()
            return fixed_date
        else:
            self.date_errors += 1
            print("Date Error")
            return self.current_date
    
    def title_normalizer(self, html):
        title = str(html.title.string)
        article_name_norm = str(unicodedata.normalize('NFKD', title).encode('ASCII', 'ignore'))[2:-1]
        name_cleaned = article_name_norm.lower().replace("- clarin", "")
        return name_cleaned.replace("-", " ")
    
    def article_extractor(self, html):
        article = "" 
        if html.find("div", class_="body-nota").findAll("p"):
            raw_article = html.find("div", class_="body-nota").findAll("p")
            for paragraph in raw_article[:-6]:
                article += paragraph.text + "\n"
            self.article_dict[self.title_normalizer(html)] = article
            return article
        elif html.findAll("p"):
            raw_article = html.findAll("p")
            for paragraph in raw_article:
                article += paragraph.text + "\n"
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
            source = requests.get(url)
            html = BeautifulSoup(source.text, "lxml")
            if hasattr(html.find("div", class_="body-nota"), "findAll"):
                d_values["date"].append(self.date_extractor(html))
                d_values["url"].append(str(url))
                d_values["article_name"].append(self.title_normalizer(html))
                d_values["topic"].append(str(url.split("/")[3]))
                self.article_extractor(html)
            else:
                print("URL dropped")
                self.url_drops += 1
                continue
        return d_values
        
    def df_generator(self, url_list):
        df = pd.DataFrame(self.df_values(url_list))
        df["media_name"] = "clarin" 
        df["extraction_date"] = self.current_date
        df.reset_index(drop=True, inplace=True)
        return df


#############
#URL SCRAPER#
#############
clarin = Clarin_scraper("https://www.clarin.com/")
clarin_html = clarin.scrap()
clarin.url_alert()

#############################
#DATA EXTRACTION/DF CREATION#
#############################
clarin_frame = Clarin_extractor()
df = clarin_frame.df_generator(clarin.url_filtered_list)


##############
#ARTICLE LOAD#
##############
json_path = f"local_repo\data_science\projects\media\clarin\data_files\clarin-articles-{clarin_frame.current_date}.json"
with open(json_path, "w") as write_file:
    json.dump(clarin_frame.article_dict, write_file, indent=4)

################
#DATAFRAME LOAD#
################
# df.to_csv(f"local_repo\data_science\projects\media\clarin\data_files\clarin_master.csv", index=False)
df.to_csv(f"local_repo\data_science\projects\media\clarin\data_files\clarin_master.csv", mode="a", index=False, header=False)
csv_daily_path = f"local_repo\data_science\projects\media\clarin\data_files\clarin-{clarin_frame.current_date}.csv"
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
    if df.shape[0] == len(clarin.url_filtered_list) - clarin_frame.url_drops:
        return f"CSV Loaded: {df.shape[0]} - file created succesfully"
    else:
        return f"CSV Rows Loaded: {df.shape[0]} - Missing rows: {(len(clarin.url_filtered_list) - clarin_frame.url_drops) - df.shape[0]} - Load Error"
    
json_status = json_load_check(loaded_df, loaded_json)
csv_status = csv_load_check(loaded_df)

##########
#LOG FILE#
##########
t1_stop = perf_counter() 

extraction_date = f"Extraction date: {pd.to_datetime('today').strftime('%Y-%m-%d %H:%M:%S')}"
url_info = f"Media URL: {clarin.base_url}\nTotal URL Extracted: {len(clarin.url_all_list)}\nURL Filtered: {len(clarin.url_filtered_list)}\nURL Residuals: {len(clarin.url_residuals_list)}\nURL Droped: {clarin_frame.url_drops}\nDate Errors: {clarin_frame.date_errors}"
url_df = f"URL Loaded DF: {str(df.shape[0])}"
time_info = f"Elapsed time: {str(t1_start)}(Start), {str(t1_stop)}(Stop) \nElapsed time during the whole program in seconds: {str(t1_stop-t1_start)}"

log = extraction_date + "\n" + url_info + "\n" + url_df + "\n" +  csv_status + "\n" + json_status + "\n" + time_info
print(log)


# with open(f"local_repo\data_science\projects\media\clarin\data_files\clarinlog.txt", "w") as file:
#     file.write(log)

with open(f"local_repo\data_science\projects\media\clarin\data_files\clarinlog.txt", "a+") as log_file:
    log_file.write("\n------------------------\n")
>>>>>>> ba38aca1ae9b58dcbbd6fe8ae43f7ec40c3b6fe9
    log_file.write(log)