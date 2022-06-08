import sched
from joblib import PrintTime
import pandas as pd
from sqlalchemy import create_engine, schema
import numpy as np
from pandasql import sqldf
import penerimaan_2022

mpninfo_conn = create_engine('mysql://sugengw07:sgwi2341@10.4.19.215/mpninfo')
psql_conn = create_engine('postgresql://postgres:sgwi2341@10.4.19.215/penerimaan')
datawarehouse_conn = create_engine('mysql://admin:A1110PF@10.4.19.15/007_datawarehouse')
pysqldf = lambda q: sqldf(q,globals())

def ppmpkm(existing_janmar22):
    queri = '''
    select
    npwp,kpp,cabang,nama,kdmap,kdbayar,masa,tahun,tanggalbayar,bulanbayar,datebayar,nominal,ntpn,
    nosk,nospm,ket,tahunsk,tahunbayar,"FULL","MAP","NAMA_AR","SEKSI","NAMA_KLU","JENIS_WP", case
    when kdmap = '411128' and kdbayar in('427','428') then 'PPS'
    when kdmap = '411128' and kdbayar ='422' then 'PKM'
    when ket in ('SPMKP dari SIDJP','PBK KIRIM SEKANTOR','PBK KIRIM BEDA KANTOR','PBK TERIMA SEKANTOR','PBK TERIMA BEDA KANTOR') THEN 'PPM'
    when tahunbayar-tahun in(0,1) and kdmap in ('411125','411126','411111','411112') and 
        kdbayar in ('200','199','310','320','390','500','501') THEN 'PPM'
    when tahunbayar-tahun in (0,1) and kdmap in ('411125','411126','411111','411112') and kdbayar ='300'
    and masa='1' and masa2=12 then 'PPM'
    when nosk is not null and nosk != '-' and nosk not like '%PBK%' and nosk != 'TIDAK DIKETAHUI'
    and ((tahunbayar % 100)-tahunsk)>0 then 'PKM'
    when tahunbayar = tahun then 'PPM'
    when (tahunbayar-tahun in(0,1) and masa='12') then 'PPM'
    when tahun>tahunbayar then 'PPM'
    else 'PKM'
    end FLAG_PPM_PKM
    from existing_janmar22
    '''
    data_ppmpkm = pysqldf(queri)
    #data_ppmpkm['tahunbayar'] = data_ppmpkm['tahunbayar'].astype('int')
    return data_ppmpkm

def ppmpkm_jenis(data_ppmpkm):
    queri = '''
    select npwp,kpp,cabang,nama,kdmap,kdbayar,masa,tahun,tanggalbayar,bulanbayar,datebayar,nominal,ntpn,
    nosk,nospm,ket,tahunsk,tahunbayar,"FULL","MAP","NAMA_AR","SEKSI","NAMA_KLU","JENIS_WP","FLAG_PPM_PKM", case
    when "FLAG_PPM_PKM"='PPM' then 'RUTIN'
    when "FLAG_PPM_PKM" ='PKM' and tahunsk not in(0,22) then 'Penagihan'
    when "FLAG_PPM_PKM" ='PKM' and tahunsk in(22) then 'Pemeriksaan'
    else 'PENGAWASAN'
    end JENIS_PPM_PKM
    from
    data_ppmpkm
    '''
    data = pysqldf(queri)
    return data


janmar = '''select  m.DIM_SBR_DATA,m.NPWP as "FULL" ,left(m.NPWP,9)as npwp,mid(m.NPWP,10,3)as kpp,right(m.NPWP,3)as cabang,m.NAMA_WP as nama , 
m.KD_MAP as kdmap,m.KD_SETOR as kdbayar,left(m.MASA_PAJAK,2) as masa ,mid(m.MASA_PAJAK,3,2) as masa2,
right(m.MASA_PAJAK,4) as tahun,day(m.TGL_SETOR) as tanggalbayar,
month(m.TGL_SETOR) as bulanbayar, m.TGL_SETOR  as datebayar, 
m.JML_SETOR as nominal, m.NTPN as ntpn,m.NO_SK as nosk,
right(NO_SK,2)as tahunsk, m.NO_SPM as nospm, year(m.TGL_SETOR) as tahunbayar
from mpn_2020 m 
where year(m.TGL_SETOR) = 2022 and m.TGL_SETOR < '2022-04-01' '''


existing_janmar22 = pd.read_sql(janmar,con=datawarehouse_conn)
existing_up = pd.read_sql('select * from public.ppmpkm2022 p  where p.bulanbayar>3',con=psql_conn)

kdmap = pd.read_sql('select * from map_polos',con=psql_conn)
existing_janmar22 = pd.merge(existing_janmar22,kdmap, left_on='kdmap', right_on='KD MAP', how='left')

existing_janmar22['FULL']=existing_janmar22['FULL'].astype('str')
mfwp = pd.read_sql('select "FULL","NAMA_WP","NAMA_AR","SEKSI","NAMA_KLU","JENIS_WP" from mfwp_sblm_042022',con=psql_conn)
existing_janmar22 = pd.merge(existing_janmar22,mfwp,on=['FULL'],how='left')

sbrdata = pd.read_sql('select "ID_SBR_DATA","KET" as ket from "ID_SBR_DATA"',con=psql_conn)
existing_janmar22 = pd.merge(existing_janmar22,sbrdata,left_on='DIM_SBR_DATA', right_on='ID_SBR_DATA',how = 'left')
ket = {'SPMKP dari SIDJP':'SPMKP','VALAS':'MPN','PBK KIRIM SEKANTOR':'PBK KIRIM','PBK KIRIM BEDA KANTOR':'PBK KIRIM',
       'PBK TERIMA BEDA KANTOR':'PBK TERIMA','PBK TERIMA SEKANTOR':'PBK TERIMA','MPN':'MPN','SPM':'SPM',
       'MANUAL':'MANUAL','MP3':'MP3'}
existing_janmar22.ket = existing_janmar22.ket.map(ket)
existing_janmar22.drop(['DIM_SBR_DATA','ID_SBR_DATA','KD MAP'],axis=1,inplace=True)

existing_janmar22['tahunsk'].replace(['00','','-'],0,inplace=True)
existing_janmar22['tahunsk'] = existing_janmar22['tahunsk'].astype('int')

data_ppmpkm = ppmpkm(existing_janmar22)
ok = ppmpkm_jenis(data_ppmpkm)

existing_2022 = ok.append(existing_up)

existing_2022.to_sql('existing_ppmpkm2022',con=psql_conn,if_exists='replace',index=False,schema='laporan')
#existing_2022.to_excel(r'D:\DATA KANTOR\BULANAN\exisitng_ppmpkm2022.xlsx',index=False)
