import requests
import json
import threading
from random import gauss
from selenium import webdriver
from time import sleep, time
from concurrent.futures import ThreadPoolExecutor, as_completed
# from requests_futures.sessions import FuturesSession
# from datetime import datetime
# import traceback
# from selenium.webdriver.common.keys import Keys
# from selenium.webdriver.support.ui import Select, WebDriverWait
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support import expected_conditions
# from webdriver_manager.chrome import ChromeDriverManager
# import platform, json

# URL = "https://www.facebook.com/groups/138823889468804"
URL = "https://www.facebook.com/groups/274403372668988"
with open("config.txt", "r") as f:
    credentials = f.readline().split(" ")
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
prefs = {
    "profile.managed_default_content_settings.images": 2,
    "download.prompt_for_download": False,
}
chrome_options.add_experimental_option("prefs", prefs)
# driver = webdriver.Chrome(
#     ChromeDriverManager().install(), chrome_options=chrome_options
# )
path = r'chromedriver\chromedriver.exe'
driver = webdriver.Chrome(
    executable_path=path, chrome_options=chrome_options
)
driver.get("https://www.facebook.com/")
email = driver.find_element_by_id("email")
passw = driver.find_element_by_id("pass")
email.send_keys(credentials[0])
passw.send_keys(credentials[1])
sleep(abs(gauss(5, 5/3)))

driver.get("https://www.facebook.com/groups/274403372668988")
sleep(abs(gauss(10, 10 / 3)))
msg_set = set()
# for el in range(200):
for el in range(10):
    if not el % 10:
        print(f"scroll #{el}")
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    sleep(abs(gauss(1, 1 / 3)))
    msg_element_list = driver.find_elements_by_css_selector(
        'div[data-ad-preview="message"]'
    )
    # print(msg_element_list)
    for msg in msg_element_list:
        msg_text = msg.text
        if msg_text:
            msg_set.add(msg_text)

with open("fbtext.json", "w") as f:
    json.dump(list(msg_set), f, indent=4)

driver.close()
