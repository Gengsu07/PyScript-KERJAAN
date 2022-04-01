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
    return data

data = mpn_union()
print(data['tahunsk'])
print(data['tahunsk'].unique())
