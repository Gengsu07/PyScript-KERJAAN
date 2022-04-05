from numpy import index_exp, isin, mod
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from pandasql import sqldf

mpninfo_conn = create_engine('mysql://sugengw07:sgwi2341@10.4.19.215/mpninfo')
psql_conn = create_engine('postgresql://postgres:sgwi2341@10.4.19.215/penerimaan')
pysqldf = lambda q: sqldf(q,globals())

def mpn_union():
    netto = '''
    select * from penerimaan_2021;
    '''
    data = pd.read_sql(netto,con=psql_conn)
    #data['datebayar'] = pd.to_datetime(data['datebayar'])
  
    data['tahun_pajak'].fillna(0, inplace=True)
    data['tahun_pajak'] = data['tahun_pajak'].astype('int64')
    data.rename(columns={'jenis_data':'ket','tgl_setor':'datebayar','no_sk':'nosk',
    'bulan_bayar':'bulanbayar','tahun_pajak':'tahun','tahun_bayar':'tahunbayar',
    'jml_setor':'nominal','kd_map':'kdmap','kd_setor':'kdbayar','masa_pajak':'masa','npwp_penyetor':'FULL'},inplace=True)
    data['ket'] = data['ket'].astype('str')
    data['tahunsk'] = data['nosk'].str[-2:]
    data['tahunsk'].fillna(0, inplace=True)
    data['tahunsk'].replace(['','ne','ui','-'],0,inplace=True)
    data['tahunsk'] = data['tahunsk'].astype('int')
    return data

def ppmpkm(data):
    queri = '''
    select
    FULL,
    nama_penyetor as nama,kdmap,kdbayar,
    masa,tahun,strftime('%d',datebayar) as tanggalbayar,bulanbayar,datebayar,nominal,
    nosk,ket,tahunbayar as tahunbayar, case
    when kdmap = '411128' and kdbayar in('427','428') then 'PPS'
    when kdmap = '411128' and kdbayar ='422' then 'PKM'
    when ket in ('SPMKP','PBK KIRIM','PBK TERIMA') THEN 'PPM'
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
    from data
    '''
    data_ppmpkm = pysqldf(queri)
    #data_ppmpkm['tahunbayar'] = data_ppmpkm['tahunbayar'].astype('int')
    return data_ppmpkm

def ppmpkm_jenis(data_ppmpkm):
    queri = '''
    select npwp,kpp,cabang,nama,kdmap,kdbayar,masa,tahun,tanggalbayar,bulanbayar,datebayar,nominal,
    nosk,ket,tahunsk,tahunbayar,"FLAG_PPM_PKM", case
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


ok = mpn_union()
#data_ppmpkm = ppmpkm(data)
#ok = ppmpkm_jenis(data_ppmpkm)

# Tambah KODE MAP
kdmap = pd.read_sql('select * from map_polos',con=psql_conn)
ok = pd.merge(ok,kdmap,left_on='kdmap',right_on='KD MAP',how='left')
ok.drop('KD MAP',axis=1,inplace=True)
#ok['datebayar'] = pd.to_datetime(ok['datebayar'])

#tambah KLU,SEKSI,AR
klu = pd.read_sql('select "FULL","NAMA_AR","SEKSI","NAMA_KLU","JENIS_WP" from mfwp',con=psql_conn)
ok = pd.merge(ok,klu, on=['FULL'],how='left')
#SAVING FILE
#ppmpkm_add.to_excel(r'D:\DATA KANTOR\SQL\ppmpkm_add.xlsx',index=False)
#ok.to_excel(r'D:\DATA KANTOR\SQL\cekppmpkm.xlsx',index=False)
ok.to_sql('mpnunion2021',con=psql_conn,if_exists='replace',index=False)
# ok.to_parquet(r'D:\DATA KANTOR\PENERIMAAN\0.2022\ppmpkm.parquet',index=False)