import pandas as pd
import os
from pathlib import Path
from sqlalchemy import create_engine

conn = create_engine('postgresql://postgres:sgwi2341@10.4.19.215/penerimaan')

def import_mpnspm():
    mpn = list(Path('MPN').glob('*.csv'))
    data_mpn = pd.DataFrame()
    for file in mpn:
        temp = pd.read_csv(file,dtype={'NPWP':'str','KPP':'str','CAB':'str',
        'PTMSPJ':'str','TGLBYR':'int','BLNBYR':'int','THNBYR':'int'})
        data_mpn = data_mpn.append(temp)

    spm = list(Path('SPM').glob('*.csv'))
    data_spm = pd.DataFrame()
    for file in spm:
        temp = pd.read_csv(file,dtype={'MASA PAJAK':'str','TANGGAL BAYAR':'str','NPWP':'str','KPP':'str','CABANG':'str'})
        data_spm = data_spm.append(temp)

    
    data_mpn.rename(columns={'THNBYR':'year','BLNBYR':'month','TGLBYR':'day'},inplace=True)


    data_mpn['MASA_PAJAK'] = data_mpn['PTMSPJ'].str[0:2]
    data_mpn['TAHUN_PAJAK'] = data_mpn['PTMSPJ'].str[4:]
    #data_mpn['TGLBYR']+data_mpn['BLNBYR']+data_mpn['THNBYR']
    data_mpn['DATEBAYAR']  = pd.to_datetime(data_mpn[['day','month','year']])


    data_spm.loc[data_spm['MASA PAJAK']=='n','MASA PAJAK'] =  data_spm['TANGGAL BAYAR'].str[4:6] 
    data_spm.loc[data_spm['MASA PAJAK']=='n','TAHUN PAJAK'] = data_spm['TANGGAL BAYAR'].str[:3]

    data_spm.loc[data_spm['MASA PAJAK']!= 'n', 'MASA PAJAK'] = data_spm['MASA PAJAK'].str[:1]
    data_spm.loc[data_spm['MASA PAJAK']!= 'n','TAHUN PAJAK'] = data_spm['MASA PAJAK'].str[4:]

    data_spm['DATEBAYAR'] = data_spm['TANGGAL BAYAR'].copy()
    data_spm['DATEBAYAR'] = pd.to_datetime(data_spm['DATEBAYAR'])
    data_spm['TAHUN_BAYAR'] = data_spm['TANGGAL BAYAR'].str[:4]
    data_spm['BULAN_BAYAR'] = data_spm['TANGGAL BAYAR'].str[4:6]
    data_spm['TANGGAL_BAYAR'] = data_spm['TANGGAL BAYAR'].str[6:]


    data_mpn = data_mpn.filter(['NPWP','KPP', 'CAB','NAMA','KDMAP','KJS',
    'MASA_PAJAK','TAHUN_PAJAK','day', 'month','year','DATEBAYAR', 
    'JUMLAH','NOSKSSP','PTNTP'])
    data_mpn = data_mpn.rename(columns={'CAB':'CABANG','year':'TAHUN_BAYAR',
    'month':'BULAN_BAYAR','day':'TANGGAL_BAYAR','NOSKSSP':'NOSK','PTNTP':'NTPN'})


    data_spm = data_spm.filter(['NPWP','KPP', 'CABANG','NAMA WAJIB PAJAK', 
    'KODE MAP','KODE BAYAR','MASA_PAJAK', 'TAHUN_PAJAK','TANGGAL_BAYAR',
    'BULAN_BAYAR','TAHUN_BAYAR','DATEBAYAR','JUMLAH BAYAR (Rp)','NO SK SSP','NTPN'])
    data_spm = data_spm.rename(columns={'NAMA WAJIB PAJAK':'NAMA',
    'KODE MAP':'KDMAP','KODE BAYAR':'KJS','JUMLAH BAYAR (Rp)':'JUMLAH','NO SK SSP':'NOSK'})


    data_mpn['JENIS'] = 'MPN'
    data_spm['JENIS'] = 'SPM'

    mpnspm = data_mpn.append(data_spm)

    mpnspm['NPWP_FULL'] = mpnspm['NPWP'] + mpnspm['KPP'] + mpnspm['CABANG']
    mpnspm['MASA_PAJAK'].fillna(0,inplace=True)
    mpnspm['TAHUN_PAJAK'].fillna(0,inplace=True)

    mpnspm[['NPWP_FULL','NPWP', 'KPP', 'CABANG','KDMAP', 'KJS']] = mpnspm[['NPWP_FULL','NPWP', 'KPP', 'CABANG','KDMAP', 'KJS']].astype('str')

    mpnspm[['MASA_PAJAK','TAHUN_PAJAK', 'TANGGAL_BAYAR', 'BULAN_BAYAR', 'TAHUN_BAYAR']] = mpnspm[['MASA_PAJAK',
    'TAHUN_PAJAK', 'TANGGAL_BAYAR', 'BULAN_BAYAR', 'TAHUN_BAYAR']].astype('int')
    conn.execute('drop table if exists appportal.mpnspm_2022 cascade')
    mpnspm.to_sql('mpnspm_2022',index=False,if_exists='replace',con=conn,schema='appportal')

def import_pbk():
    pbk = list(Path('PBK').glob('*.csv'))
    data_pbk = pd.DataFrame()
    for file in pbk:
        temp = pd.read_csv(file, parse_dates=['TGL_DOKUMEN','TGL_BERLAKU'],
        dtype= {'NPWP':'str', 'KPP':'str', 'CAB':'str','KPPADM_LB':'str', 'NPWP.1':'str','KPP.1':'str',
        'CAB.1':'str','MAP.1':'str','KD SETOR.1':'str','MAP':'str','MAP.1':'str','KD SETOR':'str'})
        data_pbk = data_pbk.append(temp)

    data_pbk['JUMLAH_PBK']= data_pbk['JUMLAH_PBK'].str.replace(',','')
    data_pbk['JUMLAH_PBK'] = data_pbk['JUMLAH_PBK'].astype('int64')

    data_pbk['TGL_DOKUMEN'] = pd.to_datetime(data_pbk['TGL_DOKUMEN'])
    data_pbk['TAHUN'] = data_pbk['TGL_DOKUMEN'].dt.year

    data_pbk.rename(columns={'NOMOR_PBK':'NOPBK','TGL_DOKUMEN':'TANGGALDOC','TGL_BERLAKU':'TANGGALBERLAKU',
    'JUMLAH_PBK':'NOMINAL','CURRENCY_PBK':'CURRENCY','TIPE_PBK':'TIPE','FG_STATUS':'STATUS','NPWP':'NPWP','KPP':'KPP',
    'CAB':'CABANG','NAMA':'NAMA','MAP':'KDMAP','KD SETOR':'KDBAYAR','MASA PAJAK':'MASA_PAJAK','TAHUN PAJAK':'TAHUN_PAJAK',
    'KPPADM_LB':'KPP_ADMIN','NPWP.1':'NPWP2','KPP.1':'KPP2','CAB.1':'CABANG2','NAMA.1':'NAMA2','MAP.1':'KDMAP2',
    'KD SETOR.1':'KDBAYAR2','MASA PAJAK.1':'MASA_PAJAK2','TAHUN PAJAK.1':'TAHUN_PAJAK2','KPPADM_KB':'KPP_ADMIN2',
    'NTPN':'NTPN','NO PROD HUKUM':'NO_PROD_HUKUM'},inplace=True)

    data_pbk = data_pbk.filter(['TAHUN','NOPBK','TANGGALDOC','TANGGALBERLAKU','NOMINAL','CURRENCY','TIPE','STATUS',
    'NPWP','KPP','CABANG','NAMA','KDMAP','KDBAYAR','MASA_PAJAK','TAHUN_PAJAK','KPP_ADMIN','NPWP2','KPP2','CABANG2','NAMA2',
    'KDMAP2','KDBAYAR2','MASA_PAJAK2','TAHUN_PAJAK2','KPP_ADMIN2','NTPN','NO_PROD_HUKUM'])


    data_pbk['NO_PROD_HUKUM'].fillna('',inplace=True)
    conn.execute('drop table if exists appportal.pbk_2022 cascade')
    data_pbk.to_sql('pbk_2022',if_exists='replace',index=False,con=conn,schema='appportal')

def import_spmkp():
    spmkp = list(Path('SPMKP').glob('*.csv'))
    data_spmkp = pd.DataFrame()
    for file in spmkp:
        temp = pd.read_csv(file, parse_dates=['TGL SPMKP'],dtype={'NPWP':'str','KD MAP':'str',
        'NO SPMKP':'str'})
        data_spmkp = data_spmkp.append(temp)

    data_spmkp['KPP'] = data_spmkp['NPWP'].str[10:13]
    data_spmkp['CABANG'] = data_spmkp['NPWP'].str[14:]
    data_spmkp['NPWP'] = data_spmkp['NPWP'].str[:9]
    data_spmkp['HARI'] = data_spmkp['TGL SPMKP'].dt.day
    data_spmkp['BULAN'] = data_spmkp['TGL SPMKP'].dt.month
    data_spmkp['NPWPFULL'] = data_spmkp['NPWP']+data_spmkp['KPP']+data_spmkp['CABANG']
    data_spmkp['NILAI SPMKP']= data_spmkp['NILAI SPMKP'].str.replace(',','')
    data_spmkp['NILAI SPMKP'] = data_spmkp['NILAI SPMKP'].astype('int64')

    data_spmkp.rename(columns={'KD MAP':'KDMAP','TGL SPMKP':'DATEBAYAR','NILAI SPMKP':'NOMINAL'},inplace=True)
    data_spmkp = data_spmkp.filter(['NPWPFULL','NPWP','KPP','CABANG','HARI','BULAN','TAHUN','DATEBAYAR','NO SPMKP',
    'KDMAP','NOMINAL','JENIS'])
    conn.execute('drop table if exists appportal.spmkp_2022 cascade')
    data_spmkp.to_sql('spmkp_2022',if_exists='replace',index=False,con=conn,schema='appportal')

