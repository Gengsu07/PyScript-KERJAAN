from playwright.sync_api import Playwright, sync_playwright
from bs4 import BeautifulSoup
from datetime import datetime


def run(playwright: Playwright) -> None:
    iphone7 = p.devices['iPhone 7']
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context(
        ** iphone7,
        locale='id_ID',
        geolocation={'latitude':-6.181564761467654,'longitude':106.83369439912856},
        permissions=['geolocation']
    )
    # Open new page
    page = context.new_page()
    # Go to https://logbook.pajak.go.id/login
    page.goto("https://logbook.pajak.go.id/login")
    # Click [placeholder="User\ SIKKA"]
    # Click [placeholder="User\ SIKKA"]
    page.click('//input[@id="nip"]')
    # Fill [placeholder="User\ SIKKA"]
    page.fill('//input[@id="nip"]', "810202558")
    # Click [placeholder="Kata\ Sandi"]
    page.click('//input[@id="password"]')
    # Fill [placeholder="Kata\ Sandi"]
    page.fill('//input[@id="password"]', "Gengsu10")
    # Click text=Masuk
    page.click("text=Masuk")
    # assert page.url == "https://logbook.pajak.go.id/Presensi"
    # Click text=<< Isi Assessment
    #page.on('dialog', dialog => dialog.dismiss());
    page.click("text=<< Isi Assessment")
    # assert page.url == "https://logbook.pajak.go.id/SelfAssessmentKesehatan/form"
    # Click [aria-label=""]
    page.click('//a[@href="#next"]')

    page.click('//*[@id="form_self_assessment"]/div[3]/ul/li[2]/a')

    page.click('//*[@id="form_self_assessment-p-1"]/div/div/div[1]/div[1]/label/span')
    page.click('//*[@id="suhu"]/div[1]/label/span')
    page.click('//*[@id="form_self_assessment"]/div[3]/ul/li[2]/a')
    page.click('//*[@id="form_self_assessment-p-2"]/div/div/div[1]/div[3]/label/span')
    page.click('//*[@id="form_self_assessment"]/div[3]/ul/li[2]/a')
    page.click('//*[@id="form_self_assessment-p-3"]/div/div/div/div[15]/label/span')
    page.click('//*[@id="form_self_assessment"]/div[3]/ul/li[2]/a')
    page.click('//*[@id="form_self_assessment-p-4"]/div/div/div/div[7]/label/span')
    page.click('//*[@id="form_self_assessment"]/div[3]/ul/li[2]/a')
    page.click('//*[@id="form_self_assessment-p-5"]/div/div/div/div[2]/label/span')
    page.click('//*[@id="form_self_assessment"]/div[3]/ul/li[2]/a')
    page.click('//*[@id="form_self_assessment-p-6"]/div/div/div/div[2]/label/span')
    page.click('//*[@id="form_self_assessment"]/div[3]/ul/li[2]/a')
    page.click('//*[@id="form_self_assessment-p-7"]/div/div/div/div[2]/label/span')
    page.click('//*[@id="form_self_assessment"]/div[3]/ul/li[2]/a')
    page.click('//*[@id="form_self_assessment-p-8"]/div/div/div/div[3]/label/span')
    page.click('//*[@id="form_self_assessment"]/div[3]/ul/li[2]/a')
    page.click('//*[@id="form_self_assessment-p-9"]/div/div/div/div[4]/label/span')
    page.click('//*[@id="form_self_assessment"]/div[3]/ul/li[2]/a')
    page.click('//*[@id="form_self_assessment-p-10"]/div[1]/div/div[1]/div[4]/label/span')
    page.click('//*[@id="pertayaan_10_donor_form"]/div[2]/label/span')
    page.click('//*[@id="form_self_assessment-p-10"]/div[2]/div/div/div/label/span')
    page.click('//*[@id="form_self_assessment"]/div[3]/ul/li[3]/a')
    
    
    # ---------------------
    page.goto("https://api.telegram.org/bot5172539524:AAELbUtbaRCojnLrDIurzEuJUxnuMBuF4oA/sendMessage?chat_id=913592537&text=SAK AMAN{}".format(datetime.today()))
    context.close()
    browser.close()
if __name__ == '__main__':
    with sync_playwright() as p:
        run(p)
    

