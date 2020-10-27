import pyodbc as db
import pandas as pd
import matplotlib.pyplot as plt
import datetime

# conn = db.connect('Driver={SQL Server};Server=EQNSQL02\\HOMOLOGASPXBANCO;Database=RIMovimentos;Trusted_Connection=yes')
# mov = pd.read_sql_query('SELECT m.*, f.CODMASTER FROM Movimentacoes m JOIN FIQs f ON m.CODFUND = f.CODFUND', conn)
# mov = pd.DataFrame(mov)
# conn.close()

mov = pd.read_excel(r'O:\CAIXAS\FLUXO\Movimentações Histórica.xlsm', sheet_name='BASE')
dict_masters = {'NIMITZ':61981, 'RAPTOR':61984, 'FALCON':61922, 'PATRIOT':61921, 'APACHE':61923, 'LANCER':63056, 'SEAHAWK':64480}
mov.columns = ['CODCOT', 'COTISTA', 'CODFUND', 'FUNDO', 'CODMASTER', 'SOLICITACAO', 'COTIZACAO', 'IMPACTO', 'OPERACAO', 'TIPO_RESGATE', 'FINANCEIRO', 'COTAS', 'ALOCADOR']
mov.replace({'CODMASTER':dict_masters}, inplace=True)
mov = mov[1:]

mov['COTIZACAO'] = pd.to_datetime(mov['COTIZACAO'])
mov['SOLICITACAO'] = pd.to_datetime(mov['SOLICITACAO'])
mov.index = mov.COTIZACAO

r180 = [62026, 63630, 63708, 63997, 63998, 63999, 64271, 64298, 64441, 64607, 64615]
def regra(row):
    if row['CODFUND']==62455:
        return 'L'
    elif row['CODFUND'] in r180:
        return 'T'
    else:
        return 'M'

mov['REGRA'] = mov.apply(lambda row: regra(row), axis=1)

def destaques(fundo, inicio, fim):
    ap = mov[(mov['OPERACAO']=='A') & (mov['CODMASTER']==fundo) &
        (mov['COTIZACAO']>=inicio) & (mov['COTIZACAO']<=fim)].groupby('ALOCADOR').sum().reset_index()
    ap = ap.set_index('ALOCADOR')
    resg = mov[(mov['OPERACAO']!='A') & (mov['CODMASTER']==fundo) &
        (mov['COTIZACAO']>=inicio) & (mov['COTIZACAO']<=fim)].groupby('ALOCADOR').sum().reset_index()
    resg = resg.set_index('ALOCADOR')

    df = ap.merge(resg, how='outer', left_index=True, right_index=True)[['FINANCEIRO_x', 'FINANCEIRO_y']]
    df = df.rename(columns={'FINANCEIRO_x':'APLICACAO', 'FINANCEIRO_y':'RESGATE'})
    df['APLICACAO']=df['APLICACAO'].fillna(0)
    df['RESGATE']=df['RESGATE'].fillna(0)
    df['NET'] = df['APLICACAO'] - df['RESGATE']
    df['RESGATE'] = -df['RESGATE']
    df = df.sort_values('NET')

    return df

nimitz_20 = destaques(61981, datetime.datetime(2020,1,1), datetime.datetime(2020,9,30))
nimitz_19 = destaques(61981, datetime.datetime(2019,1,1), datetime.datetime(2019,12,31))
nimitz_18 = destaques(61981, datetime.datetime(2018,1,1), datetime.datetime(2018,12,31))
nimitz = nimitz_19.merge(nimitz_20, how='outer', left_index=True, right_index=True)
nimitz = nimitz[['NET_x', 'APLICACAO_y', 'RESGATE_y', 'NET_y']]
nimitz = nimitz.rename(columns={'NET_x':'NET_19', 'APLICACAO_y':'APLICACAO', 'RESGATE_y':'RESGATE', 'NET_y':'NET'})

nimitz_18 = nimitz_18.rename(columns={'NET':'NET_18'})
nimitz_18 = nimitz_18['NET_18']
nimitz = nimitz.merge(nimitz_18, how='outer', left_index=True, right_index=True)
nimitz = nimitz[['NET_18', 'NET_19', 'APLICACAO', 'RESGATE', 'NET']]


rap_20 = destaques(61984, datetime.datetime(2020,1,1), datetime.datetime(2020,9,30))
rap_19 = destaques(61984, datetime.datetime(2019,1,1), datetime.datetime(2019,12,31))
rap_18 = destaques(61984, datetime.datetime(2018,1,1), datetime.datetime(2018,12,31))
rap = rap_19.merge(rap_20, how='outer', left_index=True, right_index=True)
rap = rap[['NET_x', 'APLICACAO_y', 'RESGATE_y', 'NET_y']]
rap = rap.rename(columns={'NET_x':'NET_19', 'APLICACAO_y':'APLICACAO', 'RESGATE_y':'RESGATE', 'NET_y':'NET'})

rap_18 = rap_18.rename(columns={'NET':'NET_18'})
rap_18 = rap_18['NET_18']
rap = rap.merge(rap_18, how='outer', left_index=True, right_index=True)
rap = rap[['NET_18', 'NET_19', 'APLICACAO', 'RESGATE', 'NET']]

fal_20 = destaques(61922, datetime.datetime(2020,1,1), datetime.datetime(2020,9,30))
fal_19 = destaques(61922, datetime.datetime(2019,1,1), datetime.datetime(2019,12,31))
fal_18 = destaques(61922, datetime.datetime(2018,1,1), datetime.datetime(2018,12,31))
fal = fal_19.merge(fal_20, how='outer', left_index=True, right_index=True)
fal = fal[['NET_x', 'APLICACAO_y', 'RESGATE_y', 'NET_y']]
fal = fal.rename(columns={'NET_x':'NET_19', 'APLICACAO_y':'APLICACAO', 'RESGATE_y':'RESGATE', 'NET_y':'NET'})

fal_18 = fal_18.rename(columns={'NET':'NET_18'})
fal_18 = fal_18['NET_18']
fal = fal.merge(fal_18, how='outer', left_index=True, right_index=True)
fal = fal[['NET_18', 'NET_19', 'APLICACAO', 'RESGATE', 'NET']]

pat_20 = destaques(61921, datetime.datetime(2020,1,1), datetime.datetime(2020,9,30))
pat_19 = destaques(61921, datetime.datetime(2019,1,1), datetime.datetime(2019,12,31))
pat_18 = destaques(61921, datetime.datetime(2018,1,1), datetime.datetime(2018,12,31))
pat = pat_19.merge(pat_20, how='outer', left_index=True, right_index=True)
pat = pat[['NET_x', 'APLICACAO_y', 'RESGATE_y', 'NET_y']]
pat = pat.rename(columns={'NET_x':'NET_19', 'APLICACAO_y':'APLICACAO', 'RESGATE_y':'RESGATE', 'NET_y':'NET'})

pat_18 = pat_18.rename(columns={'NET':'NET_18'})
pat_18 = pat_18['NET_18']
pat = pat.merge(pat_18, how='outer', left_index=True, right_index=True)
pat = pat[['NET_18', 'NET_19', 'APLICACAO', 'RESGATE', 'NET']]

apa_20 = destaques(61923, datetime.datetime(2020,1,1), datetime.datetime(2020,9,30))
apa_19 = destaques(61923, datetime.datetime(2019,1,1), datetime.datetime(2019,12,31))
apa_18 = destaques(61923, datetime.datetime(2018,1,1), datetime.datetime(2018,12,31))
apa = apa_19.merge(apa_20, how='outer', left_index=True, right_index=True)
apa = apa[['NET_x', 'APLICACAO_y', 'RESGATE_y', 'NET_y']]
apa = apa.rename(columns={'NET_x':'NET_19', 'APLICACAO_y':'APLICACAO', 'RESGATE_y':'RESGATE', 'NET_y':'NET'})

apa_18 = apa_18.rename(columns={'NET':'NET_18'})
apa_18 = apa_18['NET_18']
apa = apa.merge(apa_18, how='outer', left_index=True, right_index=True)
apa = apa[['NET_18', 'NET_19', 'APLICACAO', 'RESGATE', 'NET']]

lan_20 = destaques(63056, datetime.datetime(2020,1,1), datetime.datetime(2020,9,30))
lan_19 = destaques(63056, datetime.datetime(2019,1,1), datetime.datetime(2019,12,31))
lan_18 = destaques(63056, datetime.datetime(2018,1,1), datetime.datetime(2018,12,31))
lan = lan_19.merge(lan_20, how='outer', left_index=True, right_index=True)
lan = lan[['NET_x', 'APLICACAO_y', 'RESGATE_y', 'NET_y']]
lan = lan.rename(columns={'NET_x':'NET_19', 'APLICACAO_y':'APLICACAO', 'RESGATE_y':'RESGATE', 'NET_y':'NET'})

lan_18 = lan_18.rename(columns={'NET':'NET_18'})
lan_18 = lan_18['NET_18']
lan = lan.merge(lan_18, how='outer', left_index=True, right_index=True)
lan = lan[['NET_18', 'NET_19', 'APLICACAO', 'RESGATE', 'NET']]

sea_20 = destaques(64480, datetime.datetime(2020,1,1), datetime.datetime(2020,9,30))
sea_19 = destaques(64480, datetime.datetime(2019,1,1), datetime.datetime(2019,12,31))
sea_18 = destaques(64480, datetime.datetime(2018,1,1), datetime.datetime(2018,12,31))
sea = sea_19.merge(sea_20, how='outer', left_index=True, right_index=True)
sea = sea[['NET_x', 'APLICACAO_y', 'RESGATE_y', 'NET_y']]
sea = sea.rename(columns={'NET_x':'NET_19', 'APLICACAO_y':'APLICACAO', 'RESGATE_y':'RESGATE', 'NET_y':'NET'})

sea_18 = sea_18.rename(columns={'NET':'NET_18'})
sea_18 = sea_18['NET_18']
sea = sea.merge(sea_18, how='outer', left_index=True, right_index=True)
sea = sea[['NET_18', 'NET_19', 'APLICACAO', 'RESGATE', 'NET']]

with pd.ExcelWriter('Destaques Aplicaçao e Resgate.xlsx') as writer:
    nimitz.to_excel(writer, sheet_name='NIMITZ')
    rap.to_excel(writer, sheet_name='RAPTOR')
    fal.to_excel(writer, sheet_name='FALCON')
    pat.to_excel(writer, sheet_name='PATRIOT')
    apa.to_excel(writer, sheet_name='APACHE')
    lan.to_excel(writer, sheet_name='LANCER')
    sea.to_excel(writer, sheet_name='SEAHAWK')


# Close the Pandas Excel writer and output the Excel file.
writer.save()