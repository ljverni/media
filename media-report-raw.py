import pandas as pd
from datetime import datetime
import numpy as np
import json
import re
from matplotlib.gridspec import GridSpec
from matplotlib.legend import Legend
import matplotlib.patches as mpatches
import seaborn as sns
from matplotlib.lines import Line2D
from matplotlib import pyplot as plt

df_clarin = pd.read_csv(r"clarin\data_files\clarin_master.csv")
df_infobae = pd.read_csv(r"infobae\data_files\infobae_master.csv")
df_lanacion = pd.read_csv(r"lanacion\data_files\lanacion_master.csv")
df_tn = pd.read_csv(r"todonoticias\data_files\todonoticias_master.csv")
df_pieces = [df_clarin, df_tn, df_infobae, df_lanacion]
df = pd.concat(df_pieces)
df.reset_index(drop=True, inplace=True)

#Media dictionary
color_dict = {"Clarin": "#A75050", "Infobae": "#B9712D", "La Nacion": "#4C4E5F", "Todo Noticias": "#739BB3"}

#Remove irrelevant topics and rows with missing values
df.drop(df[(df["topic"]=="fotos") | (df["topic"]=="brandstudio")].index, inplace=True)
df.dropna(inplace=True)

#Change date values to type datetime
df["extraction_date"] = df["extraction_date"].apply(lambda x: datetime.strptime(str(x), "%Y-%m-%d"))
df["extraction_date"] = df["extraction_date"].dt.date

#Create covid-19 column
df["covid_in_article"] = None
for name in df["media_name"].unique():
    for d in list(df["extraction_date"].unique()):
        json_path = fr"{name}\data_files\{name}-articles-{d}.json"
        with open(json_path, "r") as json_file:
            loaded_json = json.load(json_file)
            for key, value in loaded_json.items():
                match = re.search(r"coronavirus|covid|cuarentena", value, flags=re.IGNORECASE)
                if match:
                    df.loc[(df["media_name"]==name)&(df["article_name"]==key), "covid_in_article"] = True
                else:
                    df.loc[(df["media_name"]==name)&(df["article_name"]==key), "covid_in_article"] = False

#Translation and capitalization
df["topic"].replace(list(df["topic"].unique()), ["society", "opinion", "economy", "politics", "international", "lifestyle", "international", "sports", "society", "international", "culture", "entertainment", "crime", "others", "entertainment", "tourism", "lifestyle", "lifestyle", "technology", "society", "cars", "society", "economy", "others", "culture", "lifestyle", "technology", "entertainment", "entertainment", "entertainment", "international", "sports", "society", "coronavirus", "others", "entertainment", "international", "entertainment", "sports", "international", "entertainment", "lifestyle", "international", "international", "culture", "others", "lifestyle", "culture", "international", "science", "culture", "others", "economy", "science", "international", "technology", "environment", "others", "entertainment", "international", "lifestyle", "opinion", "crime", "others", "society", "society", "lifestyle", "tourism"], inplace=True)
df["topic"] = df["topic"].apply(lambda x: x.capitalize())
df["media_name"].replace(list(df["media_name"].unique()), ["Clarin", "Todo Noticias", "Infobae", "La Nacion"], inplace=True)

#Analytics
plt.style.use("seaborn")
#DATE LABELS LIST
date_labels = [pd.to_datetime(date).strftime("%m-%d") for date in list(df["extraction_date"].unique())]
#DATES LIST
dates = list(df["extraction_date"].unique())
#COLORS
color_dict = {"Clarin": "#B48686", "Infobae": "#E19F3F", "La Nacion": "#6D6D75", "Todo Noticias": "#88B9AA"}
default_colors = [color_dict[key] for key in color_dict]
#MEDIA NAMES
media_names = sorted(list(df["media_name"].unique()))

def tick_spacer(interval):
    spaced_ticks = []
    for n, date in enumerate(date_labels):
        if n%interval==0:
            spaced_ticks.append(date)
        else:
            spaced_ticks.append("")
    return spaced_ticks

class Plotter:
    def __init__(self, query, title):
        self.query = query
        self.title = title
        
    def line_plotter(self, default_colors=None):
        ax.plot(self.query, linewidth=3)
        ax.set_prop_cycle('color', default_colors)
        ax.set_xticks(dates), ax.set_xticklabels(date_labels)
        ax.set_facecolor("#D5D8DC")
        ax.set_title(self.title, position=[.5, 1.05], fontsize=12)
        ax.legend(media_names, bbox_to_anchor=(0.5,1.09), loc="upper center", ncol=4) 
        
    def box_plotter(self):
        sns.boxplot(self.query.iloc[:, 1], self.query.iloc[:, 2], data=self.query, linewidth=2.5).set(xlabel="", ylabel="")
        idx = 0
        for name in media_names:
            box = ax.artists[idx]
            box.set_facecolor(default_colors[idx])
            idx += 1
        ax.set_facecolor("#D5D8DC")
            
    def bar_plotter(self, color):
        ax.bar(list(self.query.index), self.query.to_list(), color=color, width=0.40)
        ax.set_title(self.title)
        for i in range(len(self.query)):
            ax.text(self.query.index[i], self.query[i]+20, f"{round((self.query[i]*100)/self.query.sum(), 2)}%",horizontalalignment="center")
        ax.set_facecolor("#D5D8DC")
        
    def bar_plotter_stack(self, color):
        x, y = list(self.query[0].index), self.query[0].to_list()
        x1, y1 = list(self.query[1].index), self.query[1].to_list()
        ax.bar(x, y, color=color[0], width=0.40)
        ax.bar(x1, y1, color=color[1], width=0.30)
        ax.set_title(self.title, fontsize=12)
        yticks = ax.get_yticks()
        yticks_list = list(yticks) + [(yticks[-1]) + (yticks[-1]) - (yticks[-2])]
        ax.set_yticks(yticks_list)
        percent_value = round(self.query[1].sum()/self.query[0].sum()*100, 1)        
        ax.legend([f"Articles total: {self.query[0].sum()}",  f"Covid-19 total: {self.query[1].sum()} ({percent_value}%)"])
        for i in range(len(x1)):
            ax.text(x1[i], y1[i]+(((yticks[-1])-(yticks[-2]))/3), f"{round((y1[i]*100)/y[i], 2)}%",horizontalalignment="center", color="#641E16")
        ax.set_facecolor("#D5D8DC")
        
    def barh_plotter(self, color):
        ax.barh(self.query.index, self.query.to_list(), color=color)
        ax.set_title(self.title, fontsize=12)
        ax.set_ylabel("")
        labels = [topic for topic in self.query.sort_values(ascending=False).head().index.to_list()]
        patches = [mpatches.Patch(color=color) for i in labels]
        plt.legend(patches, labels, title="Top 5 topics", loc="lower right")
        ax.set_facecolor("#D5D8DC")
        return ax

###FIGURE-A###
fig = plt.figure(figsize=(15, 10))
gs = GridSpec(2, 2)

#QUERIES
values_bymedia = df.groupby(["media_name"])["article_name"].count().sort_values(ascending=False)
values_bydate_bymedia = df.groupby(["extraction_date", "media_name"]).agg({"article_name":"count"})
outlier_info = values_bydate_bymedia.reset_index().groupby("media_name").get_group("Infobae").sort_values(by="article_name", ascending=False).iloc[0].values
outlier_tn = values_bydate_bymedia.reset_index().groupby("media_name").get_group("Todo Noticias").sort_values(by="article_name", ascending=False).iloc[0].values
mean_total = int(np.average(values_bydate_bymedia["article_name"].values))
sd = [name + ": " + str(round(np.std(values_bydate_bymedia.reset_index().sort_values(by="article_name").groupby("media_name").get_group(name)["article_name"].values), 1)) for name in media_names]

####LINE PLOT####
ax = fig.add_subplot(gs[0, :2])
articles_bydate1 = Plotter(values_bydate_bymedia.unstack(), "Articles by day")
articles_bydate1.line_plotter(default_colors)

####BOX PLOT####
ax = fig.add_subplot(gs[1, 0])
articles_bydate2 = Plotter(values_bydate_bymedia.reset_index(), "Daily articles total distribution")
articles_bydate2.box_plotter()
ax.set_title("Daily distribution", fontsize=12)

####BAR PLOT####
ax = fig.add_subplot(gs[1, 1])
articles_total = Plotter(values_bymedia, "Total samples")
articles_total.bar_plotter("#4A584A")

plt.show()

###FIGURE-B###
fig = plt.figure(figsize=(15, 10))
gs = GridSpec(2, 2)  

#QUERIES         
total_bymedia_bytopic = df.groupby(["media_name", "topic"]).agg({"article_name":"count"}).reset_index(level="topic").rename(columns={"article_name":"topic_totals"})
df_bytopic_merged = pd.merge(left=total_bymedia_bytopic, right=values_bymedia, how="inner", on="media_name").rename(columns={"article_name":"media_totals"})
df_bytopic_merged["topic_percent"] = round((df_bytopic_merged["topic_totals"] / df_bytopic_merged["media_totals"])*100, 2)
total_perc_bytopic = df_bytopic_merged.drop(columns="media_totals").reset_index()
df_ex = lambda x, y: total_perc_bytopic.groupby(["media_name", "topic"]).get_group((x, y))[["topic_totals", "topic_percent"]].values[0][1]

####BARH PLOT####
idx  = 0
for name in media_names:
    total_bytopic = df.loc[df["media_name"]==name]["topic"].value_counts().sort_values()
    ax = fig.add_subplot(gs[idx])
    p_topic = Plotter(total_bytopic, f"Articles by topic - {name}")
    p_topic.barh_plotter(color_dict[name])
    idx += 1
    
plt.show()

###FIGURE-C###
fig = plt.figure(figsize=(15, 4))
gs = GridSpec(1, 2)

#QUERIES
df_covid_total = df.loc[(df["article_name"].str.contains("coronavirus|cuarentena|covid"))|(df["covid_in_article"]==True)]
covid_bytopic = df_covid_total.groupby(["media_name", "topic"]).agg({"article_name":"count"}).rename(columns={"article_name":"covid_totals"})
ttl_cvd_bm_bt = pd.merge(total_bymedia_bytopic.reset_index(), covid_bytopic, on=("media_name", "topic"))
ttl_cvd_bm = ttl_cvd_bm_bt.groupby("media_name")[["topic_totals", "covid_totals"]].sum()
ttl_cvd_bm["%"] = round((ttl_cvd_bm["covid_totals"] / ttl_cvd_bm["topic_totals"])*100, 2) 
cvd_perc_over_ttl = round((ttl_cvd_bm["covid_totals"].sum() / ttl_cvd_bm["topic_totals"].sum())*100, 2)

df_covid_bydate = df_covid_total.groupby("extraction_date").agg({"article_name":"count"})
df_bydate = df.groupby("extraction_date").agg({"article_name":"count"})

####BAR STACKED PLOT####
ax = fig.add_subplot(gs[0])
corona = Plotter([ttl_cvd_bm["topic_totals"], ttl_cvd_bm["covid_totals"]], "Covid-19 in articles")
corona.bar_plotter_stack(["#A9CCE3", "#EC7063"])

####LINE PLOT####
ax = fig.add_subplot(gs[1])
covid_bydate = Plotter(df_bydate, "Covid-19 in articles by day")
covid_bydate.line_plotter("#A9CCE3")
ax.plot(df_covid_bydate, color="#EC7063", linewidth=3)
ax.legend(["All articles", "Covid-19"], bbox_to_anchor=(0.5,1.09), loc="upper center", ncol=4) 
ax.set_xticklabels(tick_spacer(3))

plt.subplots_adjust(hspace=0.4)
plt.show()

###FIGURE-D###
fig = plt.figure(figsize=(15, 25))
gs = GridSpec(4, 1)  

###QUERIES
ttl_cvd_bt = ttl_cvd_bm_bt.groupby("topic")[["topic_totals", "covid_totals"]].sum()
ttl_cvd_bt["%"] = round((ttl_cvd_bt["covid_totals"] / ttl_cvd_bt["topic_totals"])*100, 2) 
ttl_cvd_bm_bt["%"] = round((ttl_cvd_bm_bt["covid_totals"] / ttl_cvd_bm_bt["topic_totals"])*100, 2) 
def get_ttl_cvd_pct(media, topic):
    values = ttl_cvd_bm_bt.groupby("media_name").get_group(media).loc[ttl_cvd_bm_bt["topic"]==topic]
    return values["topic_totals"].item(), values["covid_totals"].item(), values["%"].item()
top10_cvd = ttl_cvd_bm_bt.loc[ttl_cvd_bm_bt["topic_totals"]>100].sort_values(by="%", ascending=False).iloc[0:10]
top_s = ""
top_lst = [f"{t} ({m}): {p}%" for m, t, p in top10_cvd[["media_name", "topic", "%"]].values]
for s in top_lst:
    top_s += s + ". "
bottm_s = ""
bottm10_cvd = ttl_cvd_bm_bt.loc[ttl_cvd_bm_bt["topic_totals"]>100].sort_values(by="%").iloc[0:10]
bottm_lst = [f"{t} ({m}): {p}%" for m, t, p in bottm10_cvd[["media_name", "topic", "%"]].values]
for s in bottm_lst:
    bottm_s += s + ". "

####BARH PLOT####
idx  = 0
for name in media_names:
    ttl_cvd_xt = ttl_cvd_bm_bt.groupby("media_name").get_group(name).drop(columns=["media_name"]).set_index(["topic"]).sort_values(by="covid_totals")
    ax = fig.add_subplot(gs[idx])
    p_topic = Plotter(ttl_cvd_xt["topic_totals"], "OVERRIDDEN")
    p_topic.barh_plotter("#ABB2B9")
    c_topic = Plotter(ttl_cvd_xt["covid_totals"], f"Covid-19 by topic - {name}")
    c_topic.barh_plotter(color_dict[name])
    idx += 1      
    for i in range(len(ttl_cvd_xt)):
        x, y  = list(ttl_cvd_xt["topic_totals"].index), ttl_cvd_xt["topic_totals"].to_list()
        x1, y1 = list(ttl_cvd_xt["covid_totals"].index), ttl_cvd_xt["covid_totals"].to_list()
        ax.text(y[i]+2, x[i], f"{round((y1[i]*100)/y[i], 2)}%", verticalalignment="center", color="#641E16")

plt.show()

###APPENDIX FIGURE-A###
fig = plt.figure(figsize=(20, 40))
gs = GridSpec(9, 1)

###MAIN TOPICS
max_per_day = df.groupby(["media_name", "topic", "extraction_date"]).agg({"article_name":"count"}).max().item()
main_topics = []
for name, groups in total_bymedia_bytopic.groupby("media_name"):
    for media, topic in groups.sort_values(by="topic_totals", ascending=False).iloc[0:6]["topic"].items():
        if topic not in main_topics:
            main_topics.append(topic)

####LINE PLOT TOPICS####
idx = 0
for topic in main_topics:
    df_topic =  df.groupby("topic").get_group(topic).groupby(["extraction_date", "media_name"]).agg({"article_name":"count"})
    ax = fig.add_subplot(gs[idx])
    articles_bydate1 = Plotter(df_topic.unstack(), f"Articles by day - {topic}")
    articles_bydate1.line_plotter(default_colors)
    ax.set_yticks(np.arange(0, max_per_day, 10))
    idx += 1
           
plt.subplots_adjust(hspace=0.4)
plt.show()

###APPENDIX FIGURE-B###
fig = plt.figure(figsize=(15, 10))
gs = GridSpec(2, 2)

###QUERIES
covid_intitle = df.loc[(df["article_name"].str.contains("coronavirus|cuarentena|covid"))]
covid_incontent = df.loc[df["covid_in_article"]==True]

####BAR STACKED PLOT####
idx = [0, 2, 1, 3]
titles = ["title", "content"]
idx = 0

for df_c in covid_intitle, covid_incontent:
    covid_and_total_bymedia = df.groupby("media_name")["article_name"].count(), df_c.groupby("media_name")["article_name"].count()
    ax = fig.add_subplot(gs[idx])
    corona = Plotter(covid_and_total_bymedia, f"Covid-19 in article {titles[idx]}")
    corona.bar_plotter_stack(["#A9CCE3", "#EC7063"])
    idx += 1
        
####LINE PLOT COVID####
df_covid =  df_covid_total.groupby(["extraction_date", "media_name"]).agg({"article_name":"count"})
ax = fig.add_subplot(gs[1, :2])
articles_bydate1 = Plotter(df_covid.unstack(), f"Articles by day - Covid-19")
articles_bydate1.line_plotter(default_colors)

plt.subplots_adjust(hspace=0.4)
plt.show()