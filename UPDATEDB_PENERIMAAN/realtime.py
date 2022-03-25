from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
import pandas as pd
import time
from sqlalchemy import create_engine

conn = create_engine('postgresql://postgres:sgwi2341@10.4.19.215/penerimaan')

opsi = Options()
opsi.headless=True
path = 'Chromedriver\chromedriver.exe'
driver = webdriver.Chrome(path,options=opsi)
driver.get('https://appportal/login/index.php')
driver.find_element_by_xpath('//input[@name="username"]').send_keys('060103687')
driver.find_element_by_xpath('//input[@name="password"]').send_keys('Tugutani007n')
driver.find_element_by_xpath('//input[@name="sublogin"]').click()

driver.get('https://appportal/realtime/rg2idr.php?p1=007%20-MADYA%20JAKARTA%20TIMUR')
ac = ActionChains(driver)
datapenerimaan = driver.find_element_by_xpath('//li[@style="z-index: 99;"]')
ac.move_to_element(datapenerimaan).perform()
mpn = driver.find_element_by_xpath('//*[@id="smoothmenu1"]/ul/li[2]/ul/li[3]/a')
ac.move_to_element(mpn).perform()
driver.find_element_by_xpath('//*[@id="smoothmenu1"]/ul/li[2]/ul/li[3]/ul/li[7]/a').click()
 
header = ['NPWP TETAP','NAMA','MAP','KJS','MASA','TGLBYR','NTPN','ID BILLING','MATA UANG','JUMLAH']
data = pd.DataFrame(columns=header)
baris = driver.find_elements_by_tag_name('tr')
""" row_data = baris[1].find_elements_by_tag_name('td')
data_awal = [x.text for x in row_data]
lenght = len(data)
data.loc[lenght] = data_awal """

for item in baris[1:-2]:
    row_data = item.find_elements_by_tag_name('td')
    data_awal = [x.text for x in row_data]
    lenght = len(data)
    data.loc[lenght] = data_awal
    
    """ for item in baris :
    rowdata = item.find_elements_by """
driver.quit()
data['JUMLAH'] = data['JUMLAH'].str.replace(",",'').astype('int')
print('======SCRAPE BERHASIL!!======')
print('========IMPORT TO DB==========')
data.to_sql('realtime',schema='appportal',index=False, if_exists='replace',con=conn)
print('========DB CREATED...DOONE==========')
