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
    SELECT admin,
    npwp,
    kpp,
    cabang,
    nama,
    kdmap,
    kdbayar,
    masa,
    masa2,
    tahun,
    tanggalbayar,
    bulanbayar,
    tahunbayar,
    datebayar,
    nominal,
    ntpn,
    bank,
    nosk,
    nospm,
    tipe,
    source,
    extra,
    billing,
    nop,
    pembuat,
    CASE WHEN SOURCE = 1 THEN 'MPN' ELSE 'SPM' END AS ket 
    FROM MPN WHERE TAHUNBAYAR=2022
    UNION ALL 
    SELECT admin,
    npwp,
    kpp,
    cabang,
    '',
    kdmap,
    '',
    '',
    '',
    '',
    DAY(tanggal) AS TANGGALBAYAR,
    BULAN,
    TAHUN,
    tanggal,
    NOMINAL*-1,
    '',
    '',
    '',
    '',
    '',
    3 AS SOURCE,
    '',
    '',
    '',
    '',
    'SPMKP' AS 'keterangan' 
    FROM spmkp WHERE TAHUN=2022
    UNION ALL 
    SELECT A.admin,
    A.npwp,
    A.kpp,
    A.cabang,
    A.nama,
    kdmap,
    kdbayar,
    masapajak,
    masapajak,
    tahunpajak,
    DAY(TANGGALDOC) AS TANGGALBAYAR,
    MONTH(TANGGALDOC) BULAN,
    YEAR(TANGGALDOC) TAHUN,
    TANGGALDOC,
    NOMINAL*-1,
    ntpn,
    '',
    nopbk,
    '',
    '',
    4 AS SOURCE,
    '',
    '',
    '',
    '',
    'PBK KIRIM' AS keterangan 
    FROM PBK A 
    WHERE admin = kpp_admin AND kpp_admin = admin and TAHUN='2022'
    UNION ALL 
    SELECT A.ADMIN,
    npwp2,
    kpp2,
    cabang2,
    nama2,
    kdmap2,
    kdbayar2,
    masapajak2,
    masapajak2,
    tahunpajak2,
    DAY(TANGGALDOC) AS TANGGALBAYAR,
    MONTH(TANGGALDOC) BULAN,
    YEAR(TANGGALDOC) TAHUN,
    TANGGALDOC,
    NOMINAL,
    ntpn,
    '',
    nopbk,
    '',
    '',
    5 AS SOURCE,
    '',
    '',
    '',
    '',
    'PBK TERIMA' AS keterangan 
    FROM PBK A 
    WHERE admin = kpp_admin2 AND kpp_admin2 = admin and TAHUN = '2022';
    '''
    data = pd.read_sql(netto,con=mpninfo_conn)
    #data['datebayar'] = pd.to_datetime(data['datebayar'])
  
    data['tahun'].replace('',0, inplace=True)
    data['tahun'] = data['tahun'].astype('int64')
    data['ket'] = data['ket'].astype('str')
    data['tahunsk'] = data['nosk'].str[-2:]
    data['tahunsk'].fillna(0, inplace=True)
    data['tahunsk'].replace(['','ne','ui','-'],0,inplace=True)
    data['tahunsk'] = data['tahunsk'].astype('int')
    return data

def ppmpkm(data):
    queri = '''
    select
    npwp,kpp,cabang,nama,kdmap,kdbayar,masa,tahun,tanggalbayar,bulanbayar,datebayar,nominal,ntpn,
    nosk,nospm,ket,tahunsk,tahunbayar, case
    when kdmap = '411128' and kdbayar in('427','428') then 'PPS'
    when kdmap = '411128' and kdbayar ='422' then 'PKM'
    when ket in ('SPMKP','PBK KIRIM','PBK TERIMA') THEN 'PPM'
    when tahunbayar-tahun in(0,1) and kdmap in ('411125','411126','411111','411112') and 
        kdbayar in ('200','199','310','320','390','500','501') THEN 'PPM'
    when tahunbayar-tahun in (0,1) and kdmap in ('411125','411126','411111','411112') and kdbayar ='300'
    and masa='1' and masa2='12' then 'PPM'
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
    select npwp,kpp,cabang,nama,kdmap,kdbayar,masa,tahun,tanggalbayar,bulanbayar,datebayar,nominal,ntpn,
    nosk,nospm,ket,tahunsk,tahunbayar,"FLAG_PPM_PKM", case
    when "FLAG_PPM_PKM"='PPM' then 'RUTIN'
    when "FLAG_PPM_PKM" ='PKM' and tahunsk not in(0,22) then 'PENAGIHAN'
    when "FLAG_PPM_PKM" ='PKM' and tahunsk in(22) then 'PEMERIKSAAN'
    when kdbayar like '5%' then 'PEMERIKSAAN'
    else 'PENGAWASAN'
    end JENIS_PPM_PKM
    from
    data_ppmpkm
    '''
    data = pysqldf(queri)
    return data


data = mpn_union()
data_ppmpkm = ppmpkm(data)
ok = ppmpkm_jenis(data_ppmpkm)

# Tambah KODE MAP
kdmap = pd.read_sql('select * from map_polos',con=psql_conn)
ok = pd.merge(ok,kdmap,left_on='kdmap',right_on='KD MAP',how='left')
#ok['datebayar'] = pd.to_datetime(ok['datebayar'])
ok['FULL'] = ok['npwp']+ok['kpp']+ok['cabang']

#tambah KLU,SEKSI,AR
klu = pd.read_sql('select "FULL","NAMA_WP","NAMA_AR","SEKSI","NAMA_KLU","KODE_KLU","JENIS_WP" from mfwp_juni2022',con=psql_conn)
ok = pd.merge(ok,klu, on=['FULL'],how='left')
ok['datebayar'] = ok['datebayar'].astype('datetime64[ns]')

klu = pd.read_sql('select * from klu',con=psql_conn)
ok  = pd.merge(ok,klu,left_on='KODE_KLU', right_on='klu_kode', how='left' )
sektor = pd.read_sql('select * from sektor',con=psql_conn)
ok = pd.merge(ok, sektor, left_on='klu_sektor', right_on='Kode',how='left')

ok.drop(['KD MAP','klu_kode','Kode','nama'],axis=1,inplace=True)
ok['nominal'] = ok['nominal'].astype('int64')

#SAVING FILE

#ok.to_excel(r'D:\DATA KANTOR\BULANAN\Penerimaan 2022_Flag.xlsx',index=False)
ok.to_sql('ppmpkm2022',con=psql_conn,if_exists='replace')