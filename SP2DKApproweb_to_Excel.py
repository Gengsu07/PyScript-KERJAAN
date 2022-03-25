import pandas as pd
from bs4 import BeautifulSoup
import requests
import os

cwd = os.chdir('/Users/sugengw07/Downloads/SP2DK/2021')
files = os.listdir(cwd)

header = ['WP','SP2DK','SP2DK belum LHP2DK','LHP2DK','LHP2DK_Selesai','LHP2DK_Usulan Pemeriksaan','LHP2DK_Usul Bukper',
            'LHP2DK_Dalam Pengawasan','LHP2DK_TA','Estimasi Potensi awal belum LHP2DK','Estimasi Potensi awal sudah LHP2DK',
            'Perubahan','Estimasi Potensi Akhir LHP2DK','Estimasi Potensi Akhir_Selesai',
            'Estimasi Potensi Akhir_Usulan Pemeriksaan','Estimasi Potensi Akhir_Usul Bukper',
            'Estimasi Potensi Akhir_Dalam Pengawasan','RDP_Realisasi','RDP_Saldo Dalam Pengawasan']
hasil =pd.DataFrame(columns=header)
for file in files:
    if file.endswith('html'):
        web = open(file,'r')
        html = web.read()
        soup = BeautifulSoup(html,'lxml')
        body =soup.tbody

        even = body.find_all('tr',class_='even')
        odd = body.find_all(lambda tag:tag.name=='tr' and tag['class']==['odd'])

        df_even = pd.DataFrame(columns=header)

        for baris in even:
            row_data = baris.find_all('td',class_='text-right')
            row = [x.text for x in row_data]
            lenght =len(df_even)
            df_even.loc[lenght] = row


        df_ar_even = pd.DataFrame(columns=['AR'])
        for x in even:
            data = x.find_all('a')
            isi = [i.text for i in data]
            df_ar_even.loc[len(df_ar_even)] = isi

        dfeven =pd.concat((df_ar_even,df_even),axis=1)

        df_odd = pd.DataFrame(columns=header)
        for baris in odd:
            row_data = baris.find_all('td',class_='text-right')
            row = [x.text for x in row_data]
            lenght =len(df_odd)
            df_odd.loc[lenght] = row

        df_ar_odd = pd.DataFrame(columns=['AR'])
        for x in odd:
            data = x.find_all('a')
            isi = [i.text for i in data]
            df_ar_odd.loc[len(df_ar_odd)] = isi

        dfodd = pd.concat((df_ar_odd,df_odd),axis=1)
        full = pd.concat((dfeven,dfodd),ignore_index=True)

        full.AR = full.AR.apply(lambda x:x[21:])

        full.replace(',','',regex=True,inplace=True)

        full.iloc[:,1:] = full.iloc[:,1:].astype(int)
        full['Tahun'] = '2021'
        full['Bulan'] =file[:-5] 
        hasil = hasil.append(full)

seksi = pd.read_excel('/Users/sugengw07/Downloads/SP2DK/2021/db_ar.xlsx',usecols=['NAMA AR','SEKSI'])
hasil = pd.merge(hasil,seksi, left_on='AR',right_on='NAMA AR',how='left')
hasil.to_excel('2021_SP2DK_JAN_JUL.xlsx',index=False)