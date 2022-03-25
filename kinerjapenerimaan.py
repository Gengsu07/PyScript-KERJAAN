from os import error
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
import pandas as pd
import time
from bs4 import BeautifulSoup

opsi = Options()
opsi.headless=False
path = 'UPDATEDB_PENERIMAAN\Chromedriver\chromedriver.exe'
driver = webdriver.Chrome(path,options=opsi)
ac = ActionChains(driver)
driver.get('https://appportal/login/index.php')
driver.find_element_by_xpath('//input[@name="username"]').send_keys('060103687')
driver.find_element_by_xpath('//input[@name="password"]').send_keys('Tugutani007n')
driver.find_element_by_xpath('//input[@name="sublogin"]').click()
datapenerimaan = driver.find_element_by_xpath('//li[@style="z-index: 99;"]')
ac.move_to_element(datapenerimaan).perform()
mpn = driver.find_element_by_xpath('//*[@id="smoothmenu1"]/ul/li[2]/ul/li[3]/a')
ac.move_to_element(mpn).perform()
driver.find_element_by_xpath('//span[@id="kinerja"]').click()

time.sleep(1)
bulanakhir = Select(driver.find_element_by_xpath('//*[@id="dd_tahun"]'))
bulanakhir.select_by_visible_text('2022')
time.sleep(1)
bulanakhir = Select(driver.find_element_by_xpath('//*[@id="bulan1"]'))
bulanakhir.select_by_visible_text('Februari')
time.sleep(1)
bulanakhir = Select(driver.find_element_by_xpath('//*[@id="bulan2"]'))
bulanakhir.select_by_visible_text('Februari')
time.sleep(1)
unit = Select(driver.find_element_by_xpath('//*[@id="kanwil"]'))
unit.select_by_visible_text('KPP Se-KANWIL')
time.sleep(1)
driver.find_element_by_xpath('//*[@id="btncari"]').click()
""" madtim= driver.find_element_by_link_text(' MADYA JAKARTA TIMUR')
madtim.location_once_scrolled_into_view """
time.sleep(2)
driver.find_element_by_xpath('//*[text()=" MADYA JAKARTA TIMUR"]').click()
time.sleep(2)

""" web = driver.page_source
data = BeautifulSoup(web,'lxml' )"""
header = ['URAIAN.','TARGET','MPN','DOLLAR','SPM','PBK KIRIM','PBK TERIMA','BRUTO','SPMKP','NETTO','NETTO LALU','PENCAPAIAN','PERTUMBUHAN']
data = pd.DataFrame(columns=header)
baris = driver.find_elements_by_tag_name('tr')
for n in baris[15:]:
    rowdata = n.find_elements_by_tag_name('td')
    isian = [x.text for x in rowdata]
    data.loc[len(data)] = isian
data.replace(',','',inplace=True,regex=True)
data.loc[:,'TARGET':'NETTO LALU'] = data.loc[:,'TARGET':'NETTO LALU'].apply(pd.to_numeric, errors='coerce')
data.to_excel(r'D:\DATA KANTOR\APPPORTAL\kinerjapenerimaan.xlsx',index=False)

driver.quit()
