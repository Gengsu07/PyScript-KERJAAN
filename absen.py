from playwright.sync_api import Playwright, sync_playwright
from bs4 import BeautifulSoup
from datetime import datetime
import pyautogui as pag


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
    page.fill('//input[@id="password"]', "Gengsu77")
    # Click text=Masuk
    page.click("text=Masuk")
    #page.click("text=Presensi Pulang")
    page.click('//*[@id="btnPresensi"]')
    
if __name__ == '__main__':
    with sync_playwright() as p:
        run(p)
    