from os import error
from socket import htonl
from nbformat import write
from requests import head
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
import pandas as pd
import time
from bs4 import BeautifulSoup
import xlsxwriter

opsi = Options()
opsi.headless=False
path = r'F:\CODE_NOTEBOOK\chromedriver.exe'
driver = webdriver.Chrome(path,options=opsi)
ac = ActionChains(driver)
driver.get('https://appportal/login/index.php')
driver.find_element_by_xpath('//input[@name="username"]').send_keys('060103687')
driver.find_element_by_xpath('//input[@name="password"]').send_keys('Tugutani007c')
driver.find_element_by_xpath('//input[@name="sublogin"]').click()
datapenerimaan = driver.find_element_by_xpath('//li[@style="z-index: 99;"]')
ac.move_to_element(datapenerimaan).perform()
mpn = driver.find_element_by_xpath('//*[@id="smoothmenu1"]/ul/li[2]/ul/li[2]/a')
ac.move_to_element(mpn).perform()
driver.find_element_by_xpath('//span[@id="kinerja"]').click()

time.sleep(2)
tahun =['2017','2018','2019','2020','2021','2021','2022']
#'2017','2018','2019','2020','2021','2021',
awalbulan = 'Januari'
akhirbulan = 'Desember'
writer = pd.ExcelWriter(r'D:\DATA KANTOR\APPPORTAL\PenerimaanAppportal.xlsx',engine='xlsxwriter')

for year in tahun:
    bulanakhir = Select(driver.find_element_by_xpath('//*[@id="dd_tahun"]'))
    bulanakhir.select_by_visible_text('{}'.format(year))
    #time.sleep(1)
    bulanakhir = Select(driver.find_element_by_xpath('//*[@id="bulan1"]'))
    bulanakhir.select_by_visible_text('{}'.format(awalbulan))
    #time.sleep(1)
    bulanakhir = Select(driver.find_element_by_xpath('//*[@id="bulan2"]'))
    bulanakhir.select_by_visible_text('{}'.format(akhirbulan))
    #time.sleep(1)
    unit = Select(driver.find_element_by_xpath('//*[@id="kanwil"]'))
    unit.select_by_visible_text('KPP Se-KANWIL')
    #time.sleep(1)
    driver.find_element_by_xpath('//*[@id="btncari"]').click()
    time.sleep(1)
    html = driver.page_source
    soup = BeautifulSoup(html,'lxml')
    tabel = soup.find_all(attrs={'id':'tabhasil'})
    header = ['No','KPP','TARGET','MPN','DOLLAR','SPM','PBK KIRIM','PBK TERIMA','BRUTO','SPMKP','NETTO','BRUTO_LALU','NETTO_LALU',
                'CAPAIAN','TUMBUH_BRUTO','TUMBUH_NETTO']
    data =pd.DataFrame(columns=header)
    baris = driver.find_elements_by_tag_name('tr')
    for n in baris[2:-1]:
        rowdata = n.find_elements_by_tag_name('td')
        isian = [x.text for x in rowdata]
        data.loc[len(data)] = isian
    data.replace(',','',inplace=True,regex=True)
    data.loc[:,'TARGET':'NETTO_LALU'] = data.loc[:,'TARGET':'NETTO_LALU'].apply(pd.to_numeric, errors='coerce')
    data.to_excel(writer,sheet_name='{}'.format(year))
writer.save()
driver.quit()
