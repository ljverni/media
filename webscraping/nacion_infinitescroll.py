from selenium import webdriver
import time
from bs4 import BeautifulSoup
import requests
import re


def scroll_down(base_url):
    driver = webdriver.Chrome("\webdrivers\chromedriver.exe")
    driver.implicitly_wait(20)
    verificationErrors = []
    accept_next_alert = True
    delay = 3
    driver.get(base_url)
    for i in range(1,20):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
    elems = driver.find_elements_by_xpath("//a[@href]")
    urls = []
    for elem in elems:
        urls.append(elem.get_attribute("href"))
    for url in urls:
        if len(url.split("/")) <= len(base_url.split("/")):
            urls = urls[1:]
        else:
            continue
    driver.quit()
    return urls
    
