from playwright.sync_api import Playwright, sync_playwright
import pyautogui as pag
from datetime import datetime
from datetime import date
import pandas as pd
from import_to_db_2022 import *



def getbulan():
    hari_ini = date.today()
    tahun = hari_ini.year
    awaltahun = date(tahun,1,1)
    listbulan = pd.period_range(awaltahun,hari_ini,freq='M').month
    bulan_get = []
    for n in listbulan:
        if n<10:
            temp = '0{}'.format(n)
            bulan_get.append(temp)
        else:
            temp = '{}'.format(n)
            bulan_get.append(temp)
    return bulan_get

def spm(page):
    page.click('//*[@id="smoothmenu1"]/ul/li[2]/a')
    
    #mpn = page.locator("#smoothmenu1 > ul > li:nth-child(2) > ul > li:nth-child(3) > a:hastext-is('MPN')")
    #mpn.hover()
    pag.moveTo(211,50,1)
    pag.moveTo(211,135,1)
    
    page.click('//*[@id="smoothmenu1"]/ul/li[2]/ul/li[3]/a')
    page.click('//*[@id="spm"]')

    page.select_option('select[name="tgl2"]','31')
    page.select_option('select[name="thn_pjk"]','{}'.format(date.today().year))
    bulan = getbulan()
    for n in bulan:
        page.select_option('select[name="bln"]','{}'.format(n))
        page.click('//*[@id="btnDownload"]')
        # Start waiting for the download
        with page.expect_download() as download_info:
            # Perform the action that initiates download
            page.click('text=Download CSV')
        download = download_info.value
        # Wait for the download process to complete
        path = download.save_as(r'SPM/SPM_{}.csv'.format(n))

def pbk(page):
    page.click('//*[@id="smoothmenu1"]/ul/li[2]/a')
    
    #mpn = page.locator("#smoothmenu1 > ul > li:nth-child(2) > ul > li:nth-child(3) > a:hastext-is('MPN')")
    #mpn.hover()
    pag.moveTo(211,50,1)
    pag.moveTo(211,135,1)
    
    page.click('//*[@id="smoothmenu1"]/ul/li[2]/ul/li[3]/a')
    page.click('//*[@id="pbk_new2020"]')
    page.select_option('select[name="idkpp"]','007')
    tahun = date.today().year
    page.select_option('select[name="tahun"]','{}'.format(tahun))
    page.locator('//*[@id="btncari"]').click()
    with page.expect_download() as download_info:
            # Perform the action that initiates download
        page.click('text=CSV')
        download = download_info.value
            # Wait for the download process to complete
        path = download.save_as(r'PBK/PBK_{}.csv'.format(tahun))

def spmkp(page):
    page.click('//*[@id="smoothmenu1"]/ul/li[2]/a')
    spmkp = page.locator(':text-is("Monitoring SPMKP")')
    spmkp.click()
    page.select_option('select[name="action"]','2')
    tahun = date.today().year
    page.select_option('select[name="tahun"]','{}'.format(tahun))
    page.locator('//*[@id="btncari"]').click()
    with page.expect_download() as download_info:
            # Perform the action that initiates download
        page.click('text=CSV')
        download = download_info.value
            # Wait for the download process to complete
        path = download.save_as(r'SPMKP/SPMKP_{}.csv'.format(tahun))


def run(playwright: Playwright) -> None:
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()

    page = context.new_page()

    page.goto("https://appportal/login/")

    page.click("input[name=\"username\"]")

    page.fill("input[name=\"username\"]", "060103687")

    page.click("input[name=\"password\"]")

    page.fill("input[name=\"password\"]", "Tugutani007b")

    # Click text=Login
    page.click("text=Login")
    """ page.hover('//li[@style="z-index: 99;"]')
    page.hover('//*[@id="smoothmenu1"]/ul/li[2]/ul/li[3]/a/text()') """
   
    page.click('//*[@id="smoothmenu1"]/ul/li[2]/a')
    
    #mpn = page.locator("#smoothmenu1 > ul > li:nth-child(2) > ul > li:nth-child(3) > a:hastext-is('MPN')")
    #mpn.hover()
    pag.moveTo(211,50,1)
    pag.moveTo(211,135,1)
    
    page.click('//*[@id="smoothmenu1"]/ul/li[2]/ul/li[3]/a')

    
    page.click('//*[@id="mpnharianrekon"]')
    
    bulan = getbulan()
    page.select_option('select[name="tgl_akhir"]','31')
    page.select_option('select[name="dd_tahun3"]','{}'.format(date.today().year))
           
    for n in bulan:
        page.select_option('select[name="bln_awal"]','{}'.format(n))
        for valuta in ['1','2','3']:
            page.select_option('select[name="valuta"]','{}'.format(valuta))
            page.click('//*[@id="btndownload"]')
            page.wait_for_timeout(2000)
            with page.expect_download() as download_info:
                    # Perform the action that initiates download
                page.click('//*[@id="download"]/a[1]')
                download = download_info.value
                # Wait for the download process to complete
                path = download.save_as(r'MPN/Bulan-{}_{}.csv'.format(n,valuta))
     # ---------------------
    spm(page)
    pbk(page)
    spmkp(page)



    context.close()
    browser.close()



if __name__ == '__main__':
    with sync_playwright() as playwright:
        run(playwright)
    import_mpnspm()
    print('MPN SPM SELESAI GAN')
    import_pbk()
    print('PBK SELESAI GAN')
    import_spmkp()
    print('SPMKP SELESAI GAN')
    print('SELESAI SEMUA GAN')