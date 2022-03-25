from numpy import index_exp, isin, mod
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from pandasql import sqldf

mpninfo_conn = create_engine('mysql://sugengw07:sgwi2341@10.4.19.215/mpninfo')
psql_conn = create_engine('postgresql://postgres:sgwi2341@10.4.19.215/penerimaan')
pysqldf = lambda q: sqldf(q,globals())

def mpn_union():
    netto = '''SELECT admin,
        npwp,
        kpp,
        cabang,
        nama,
        kdmap,
        kdbayar,
        masa,
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
        CASE
            WHEN SOURCE = 1 THEN 'MPN'
            ELSE 'SPM'
        END AS ket
    FROM MPN
    WHERE (tahunbayar) = '2022'
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
        'SPMKP' AS ''
    FROM spmkp
    WHERE (TAHUN) = '2022'
    UNION ALL
    SELECT A.admin,
        A.npwp,
        A.kpp,
        A.cabang,
        A.nama,
        kdmap,
        kdbayar,
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
        'PBK KIRIM' AS ''
    FROM PBK A
    INNER JOIN MASTERFILE B ON A.NPWP = B.NPWP
    WHERE YEAR(TANGGALDOC) = '2022'
    AND   A.KPP = B.KPP
    AND   A.CABANG = B.CABANG
    UNION ALL
    SELECT A.ADMIN,
        npwp2,
        kpp2,
        cabang2,
        nama2,
        kdmap2,
        kdbayar2,
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
        'PBK TERIMA' AS ''
    FROM PBK A
    INNER JOIN MASTERFILE B ON A.NPWP2 = B.NPWP
    WHERE YEAR(TANGGALDOC) = '2022'
    AND   A.KPP2 = B.KPP
    AND   A.CABANG2 = B.CABANG;'''

    data = pd.read_sql(netto,con=mpninfo_conn)
    data['datebayar'] = pd.to_datetime(data['datebayar'])
  
    data['tahun'].replace('',0, inplace=True)
    data['tahun'] = data['tahun'].astype('int64')
    data['ket'] = data['ket'].astype('str')
    data['tahunsk'] = data['nosk'].str[-2:]
    data['tahunsk'].fillna(0, inplace=True)
    data['tahunsk'].replace(['','ui','-'],0,inplace=True)
    data['tahunsk'] = data['tahunsk'].astype('int')
    return data

def ppmpkm(data):
    queri = '''
    select
    npwp,kpp,cabang,nama,kdmap,kdbayar,masa,tahun,tanggalbayar,bulanbayar,datebayar,nominal,ntpn,
    nosk,nospm,ket,tahunsk,cast(strftime('%Y',datebayar) as INTEGER)as tahunbayar, case
    when kdmap = '411128' and kdbayar ='422' then 'PKM'
    when ket in ('SPMKP','PBK KIRIM','PBK TERIMA') THEN 'PPM'
    when tahunbayar-tahun in(0,1) and kdmap in ('411125','411126','411111','411112') and 
        kdbayar in ('200','199','310','320','390','500','501') THEN 'PPM'
    when tahunbayar-tahun in (0,1) and kdmap in ('411125','411126','411111','411112') and kdbayar ='300'
    and masa='1' then 'PPM'
    when nosk != '-' and ((tahunbayar % 100)-tahunsk)>0 then 'PKM'
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

def ppmpkm_next(data):
    filter = (data['kdmap'].isin(['411125','411126','411111','411112']))&(data['kdbayar']=='300') &(data['masa']=='1') &(data['tahun']!='2022')
    data.where(filter)['FLAG_PPM_PKM'] = 'PKM'
    return data

def ppmpkm_jenis(ppmpkm_add):
    queri = '''
    select npwp,kpp,cabang,nama,kdmap,kdbayar,masa,tahun,tanggalbayar,bulanbayar,datebayar,nominal,ntpn,
    nosk,nospm,ket,tahunsk,tahunbayar,"FLAG_PPM_PKM", case
    when "FLAG_PPM_PKM"='PPM' then 'RUTIN'
    when "FLAG_PPM_PKM" ='PKM' and tahunsk not in(0,22) then 'SKP / STP sd 2021'
    when "FLAG_PPM_PKM" ='PKM' and tahunsk in(22) then 'SKP / STP sd 2022'
    else 'PENGAWASAN'
    end JENIS_PPM_PKM
    from
    ppmpkm_add
    '''
    data = pysqldf(queri)
    return data


data = mpn_union()
data_ppmpkm = ppmpkm(data)
ppmpkm_add = ppmpkm_next(data_ppmpkm)
ok = ppmpkm_jenis(ppmpkm_add)

# Tambah KODE MAP
kdmap = pd.read_sql('select * from map_polos',con=psql_conn)
ok = pd.merge(ok,kdmap,left_on='kdmap',right_on='KD MAP',how='left')
ok.drop('KD MAP',axis=1,inplace=True)
#ppmpkm_add.to_excel(r'D:\DATA KANTOR\SQL\ppmpkm_add.xlsx',index=False)
ok.to_excel(r'D:\DATA KANTOR\SQL\ppmpkm.xlsx',index=False)