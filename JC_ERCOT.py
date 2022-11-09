import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import zipfile 
from urllib.request import urlretrieve
import pandas as pd
import sqlite3
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

##start_time = time.time()

PATH = r"C:\Users\usuario\OneDrive\Bots Telegram\chromedriver.exe"
s = Service(PATH)
driver = webdriver.Chrome(service=s)

url_1 = 'https://www.ercot.com/gridinfo/load/load_hist'
response_1 = requests.get(url_1)
soup_1 = BeautifulSoup(response_1.content, 'html.parser')

url_2 = 'https://www.ercot.com/mp/data-products/markets/real-time-market?id=NP6-345-CD'
response_2 = requests.get(url_2)
soup_2 = BeautifulSoup(response_2.content, 'html.parser')

cols = ['date_raw','COAST','EAST','FAR_WEST','NORTH','NORTH_C','SOUTHERN','SOUTH_C','WEST','TOTAL','date']
cols_2 = ['date','COAST','EAST','FAR_WEST','NORTH','NORTH_C','SOUTHERN','SOUTH_C','WEST','TOTAL']

#most recent data to be scraped with selenium
driver.get(url_2)
element = WebDriverWait(driver,3000).until(EC.presence_of_element_located((By.XPATH, "/html/body/div/div/div/div/div/div[3]/div[1]/div/div[2]/div/div/div/table/tbody/tr[5]")))
table_2 = driver.find_element(By.TAG_NAME, 'tbody')
rows_2 = table_2.find_elements(By.TAG_NAME, 'tr')
first = True
for row in rows_2:
    col = row.find_elements(By.TAG_NAME, 'td')[0].text[-3:]
    if col == 'csv':
        filename = row.find_elements(By.TAG_NAME, 'td')[3].find_element(By.TAG_NAME, 'a').get_attribute('href')
        with zipfile.ZipFile(urlretrieve(filename)[0], mode="r") as zzip:
            df = pd.read_csv(zzip.open(zzip.namelist()[0])) #unzip and make df
            df = df.drop(df.columns[-1], axis=1) #drop last column
            df = df.groupby(df.columns[0]).mean().reset_index() #average hourly values
            df[df.columns[0]] = [datetime.strptime(x,'%m/%d/%Y').strftime('%Y-%m-%d') for x in df[df.columns[0]]] #format date
            df.columns = cols_2 #unify column names
            if first == True:
                dfx = df
                first = False
            else:
                dfx = pd.concat([dfx, df], ignore_index=True) #condense in a DF
driver.quit()

#historical, monthly values to be scraped with Bsoup
for divs in soup_1.findAll('div', {'class' : "row wrapper ml-0 pt-2 pb-2"}):
    filedate = int(divs.div.div.div.a.text[:4])
    filename = divs.div.div.div.a.attrs['href']
    if filedate >= 2002: #only >2002
        if filename[-3:] == 'zip': #case it is a ZIP
            with zipfile.ZipFile(urlretrieve(filename)[0], mode="r") as zzip:
                df = pd.read_excel(zzip.read(zzip.namelist()[0]), parse_dates=[0])
        elif filename[-3:] == 'xls':#case it is an XLS
            df = pd.read_excel(urlretrieve(filename)[0], parse_dates=[0])
        if df[df.columns[0]].dtype != object: #if dates recorded with date format
            df['date'] = [(x- timedelta(minutes=30)).strftime('%Y-%m-%d') for x in df[df.columns[0]]]
        else: #if dates recorded as string
            df['date'] = [(datetime.strptime(x.replace(' DST','').replace('24:00','23:59'),'%m/%d/%Y %H:%M')- timedelta(minutes=30)).strftime('%Y-%m-%d') for x in df[df.columns[0]]]

        df.columns = cols #unify column names
        df = df.groupby('date').mean().reset_index() #average hourly values
        dfx = pd.concat([dfx, df], ignore_index=True) #condense in a DF

dfx = dfx.drop_duplicates(subset=['date']) #delete duplicates, new data prevails
dfx.sort_values(by=['date'], inplace=True, ascending=False)

dbname = 'ERCOT_jc.sqlite'
conn = sqlite3.connect(dbname) #DB connection and table creation
table_creation= "CREATE TABLE IF NOT EXISTS ERCOT_load (date date, COAST real, EAST real, FAR_WEST real, NORTH real, NORTH_C real, SOUTHERN real, SOUTH_C real, WEST real, TOTAL real)"
conn.execute(table_creation)
conn.commit()
#temporal table to insert only new values
table_creation= "CREATE TABLE IF NOT EXISTS temp_ERCOT_load (date date, COAST real, EAST real, FAR_WEST real, NORTH real, NORTH_C real, SOUTHERN real, SOUTH_C real, WEST real, TOTAL real)"
conn.execute(table_creation)
conn.commit()

#delete last 32 records to update new data
sql = """delete from ERCOT_load 
        where [date] in (select [date] from ERCOT_load ORDER BY [date] limit 32)"""
conn.execute(sql)
conn.commit()

dfx.to_sql('temp_ERCOT_load', conn, if_exists='append', index = False)
#insert only new rows
sql = """INSERT INTO ERCOT_load 
        SELECT t1.*
        FROM temp_ERCOT_load t1
        WHERE NOT EXISTS 
        (
              SELECT 1 
              FROM ERCOT_load t2 WHERE 
              t2.[date] = t1.[date]
        )"""
conn.execute(sql)
conn.commit()

#delete temporal table
sql = """DELETE FROM temp_ERCOT_load""" 
conn.execute(sql)
conn.commit()

##end_time = time.time()
##print('Execution time = %.6f seconds' % (end_time-start_time))
