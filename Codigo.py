# -*- coding: utf-8 -*-
"""
Created on Thu Nov  9 14:30:13 2023

@author: jcgarciam
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time 

now = datetime.now()

fec_cierre = now.date() - timedelta(days = now.day)
mes_cierre = fec_cierre.strftime('%m')
mes_cierre_anterior = (fec_cierre - timedelta(days = fec_cierre.day)).strftime('%m')
anio_cierre = fec_cierre.strftime('%Y')
anio_cierre_corto = fec_cierre.strftime('%y')

anio_cierre_anterior = (fec_cierre - timedelta(days = fec_cierre.day)).strftime('%Y')
anio_cierre_anterior_corto = (fec_cierre - timedelta(days = fec_cierre.day)).strftime('%y')


path_int1 = r'\\dc1pvfnas1\Autos\Soat_Y_ARL\Reservas ARL y Salud\Cierre\Reservas'
path_int2 = r'\\dc1pvfnas1\Autos\Soat_Y_ARL\Pagos_Arl_Salud\Requerimientos\Externos\1. Circular 035'

#%%
def ConvertirMes(mes):
    m = {
        '01': "Enero",
        '02': "Febrero",
        '03': "Marzo",
        '04': "Abril",
        '05': "Mayo",
        '06': "Junio",
        '07': "Julio",
        '08': "Agosto",
        '09': "Septiembre",
        '10': "Octubre",
        '11': "Noviembre",
        '12': "Diciembre"
        }
    return str(m[mes])

def EstandarizarFormatos(df, a = 'a'):
    df[a] = df[a].astype(str).str.strip()
    df[a] = np.where(df[a].str[-2::] == '.0',df[a].str[0:-2], df[a])
    df[a] = np.where(df[a] == 'nan',np.nan, df[a])
    return df[a]

def Formatos_sin_sufijo(df, a = 'a'):
    df[a] = df[a].astype(str).str.strip()
    df[a] = np.where(df[a].str.upper().str[0:2].isin(['SS','SF']) == True,df[a].str[2::], df[a])
    df[a] = np.where(df[a] == 'nan',np.nan, df[a])
    return df[a]

#%%

path1_int_final = path_int1 + r'/' + anio_cierre + r'/' + mes_cierre + ' ' + ConvertirMes(mes_cierre) + ' ' + anio_cierre
path2_int_final = path_int2 + r'/' + anio_cierre + r'/' + mes_cierre + ' ' + ConvertirMes(mes_cierre) + ' temp'
path3_int_final = path_int2 + '/' + anio_cierre_anterior + '/' + mes_cierre_anterior + ' ' + ConvertirMes(mes_cierre_anterior)


path_out1 = path_int2 + '/' + anio_cierre + '/' +  mes_cierre + ' ' + ConvertirMes(mes_cierre)

#%%

##### EXTRACCION DE ARCHIVOS ###
print('Extrayendo archivos para la reserva del mes: ', ConvertirMes(mes_cierre) + ' ' + anio_cierre + '\n')

columnas = ['Tipo de siniestro', 'Regional', 'Afiliación', 'Siniestro','Identificación', 
            'Nombre del Trabajador','Ocurrencia del siniestro\ndd/mm/aaaa','Aviso del siniestro\ndd/mm/aaaa', 
            'Reapertura','Fecha de reserva','Tipo prestación requerida (hechos de la demanda)',
            'Fecha de nacimiento del trabajador','Estado en que se encuentra la reclamación',
            'Observaciones reserva','IBL', 'Honorario Inicial Abogado', 'd', 'Tipo de prestación',
            'Casos con doble proceso', 'Tiene reserva PI o PS en rentas','Nueva reserva honorarios con liberación de pagos',
            'Total reserva prestaciones', 'Tipo reserva prestaciones','Total de Reserva', 'Estado', 
            'Instancia', 'Tipo de Fallo','CODIGO CIANI', 'Total de Reserva mes anterior',
            'Movimiento Mes','Tipo de Movimiento', 'Zona', 'Subgerencia Tecnica', 'Observacion']

print('Leyendo archivo Reserva Judiciales '+ ConvertirMes(mes_cierre) + ' ' + anio_cierre)
Reserva_Judiciales = pd.read_excel(path1_int_final + '/Reserva Judiciales ' + ConvertirMes(mes_cierre) + ' ' + anio_cierre + '.xlsx', sheet_name = 'Reserva Judiciales', usecols = columnas)
print('Archivo Reserva Judiciales '+ ConvertirMes(mes_cierre) + ' ' + anio_cierre + ' leído \n')

#%%

columnas = ['SINIESTRO','SALDO ACTUAL ASISTENCIAL','SALDO ACTUAL IT']

print('Leyendo archivo: ',mes_cierre + ' Consolidado reserva IT asistencial ' + ConvertirMes(mes_cierre) + ' ' + anio_cierre)
reserva_IT_Asistencial = pd.read_excel(path_int1 + '/' + anio_cierre + '/IT y Asistencial/' + mes_cierre + ' Consolidado reserva IT asistencial ' + ConvertirMes(mes_cierre) + ' ' + anio_cierre + '.xlsx', sheet_name = 'Consolidado', header = 0, usecols = columnas)
print('Archivo ', mes_cierre + ' Consolidado reserva IT asistencial ' + ConvertirMes(mes_cierre) + ' ' + anio_cierre, ' leído \n')

#%%
columnas = ['NUMERO DE SINIESTRO','RESERVA IPP ACTUAL']
print('Leyendo archivo: Reserva de IPP ' + mes_cierre + '-' + ConvertirMes(mes_cierre) + ' ' + anio_cierre)
reserva_IPP = pd.read_excel(path_int1 + '/' + anio_cierre + '/IPP/' + 'Reserva de IPP ' + mes_cierre + '-' + ConvertirMes(mes_cierre) + ' ' + anio_cierre + '.xlsx',
                            sheet_name = 'Consolidado', header = 0, usecols = columnas, dtype = {'NUMERO DE SINIESTRO':str})
print('Archivo Reserva de IPP ' + mes_cierre + '-' + ConvertirMes(mes_cierre) + ' ' + anio_cierre + ' leído \n')

#%%
print('Leyendo archivo: Formato 394 _Diagnostico_de_calidad_reserva_matemática_ARL_' + ConvertirMes(mes_cierre) + ' ' + anio_cierre)
Formato_394_Diagnostico_de_calidad_reserva_mat_ARL = pd.read_excel(path1_int_final + '/Soportes/Formato 394 _Diagnostico_de_calidad_reserva_matemática_ARL_' + ConvertirMes(mes_cierre) + ' ' + anio_cierre + '.xlsx', sheet_name = 'FORMA394', header = 0)
print('Archivo Formato 394 _Diagnostico_de_calidad_reserva_matemática_ARL_' + ConvertirMes(mes_cierre) + ' ' + anio_cierre + ' leído \n')

#%%
columnas = ['10-No. de siniestro','12-Origen de la pensión','27-Mesada','11-Fecha de siniestro',
            '28-Número de Mesadas','06-Interés Técnico','18-No. de identif.','44-No. de identif.',
            '20-Fecha de Nacimiento','46-Fecha de Nacimiento','19-Sexo','45-Sexo','23-Estado',
            '49-Estado','16-Parent. 01','42-Parent. 02','52-No. de identif.','54-Fecha de Nacimiento',
            '53-Sexo','57-Estado','50-Parent. 03','60-No. de identif.','62-Fecha de Nacimiento',
            '61-Sexo','65-Estado','58-Parent. 04','86-Constituida a sep/10','87-Se amortiza',
            ]
print('Leyendo archivo: Formato 394 avisados Diagnostico_de_calidad_reserva_avisados_ARL_' + ConvertirMes(mes_cierre) + ' ' + anio_cierre)
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL = pd.read_excel(path1_int_final + '/Soportes/Formato 394 avisados Diagnostico_de_calidad_reserva_avisados_ARL_' + ConvertirMes(mes_cierre) + ' ' + anio_cierre + '.xlsx',
                                                sheet_name = 'FORMA394', header = 0,
                                                usecols = columnas)
#Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL = Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL.drop(columns = ['Fecha_Calculo','TIPO DE RENTA'])
print('Archivo Formato 394 avisados Diagnostico_de_calidad_reserva_avisados_ARL_' + ConvertirMes(mes_cierre) + ' ' + anio_cierre + ' leído \n')

#%%
columnas = ['No. Siniestro','Fecha de Estructuración']

print('Leyendo archivo: Validación Reserva Matematica ' +  ConvertirMes(mes_cierre) + ' ' +  anio_cierre)
Validación_Reserva_Matematica = pd.read_excel(path1_int_final + '/Soportes/Validación Reserva Matemática ' +  ConvertirMes(mes_cierre) + ' ' +  anio_cierre + '.xlsx', sheet_name = 'Matemática', header = 2, usecols = columnas)
print('Archivo Validación Reserva Matematica ' +  ConvertirMes(mes_cierre) + ' ' +  anio_cierre + ' leído \n')

#%%
columnas = ['No. Siniestro','IBL','% PCL','FECHA ESTRUCTURACION']
print('Leyendo archivo: Validación Reserva Avisados ' +  ConvertirMes(mes_cierre) + ' ' +  anio_cierre)
Validacion_Reserva_Avisados = pd.read_excel(path1_int_final + '/Soportes/Prestaciones/Reserva avisados ' +  ConvertirMes(mes_cierre) + ' ' +  anio_cierre + '.xlsx', sheet_name = 'Nuevos', usecols = columnas)
print('Archivo Validación Reserva Avisados ' +  ConvertirMes(mes_cierre) + ' ' +  anio_cierre + ' leído \n')

#%%
print('Leyendo archivo: ', fec_cierre.strftime('%m') + ' Reserva de Honorarios a Juntas a ' +  ConvertirMes(mes_cierre) + ' ' + anio_cierre)
honorarios_juntas = pd.read_excel(path_int1 + r'/' + anio_cierre + r'\Honorarios a Juntas/' + fec_cierre.strftime('%m') + ' Reserva de Honorarios a Juntas a ' +  ConvertirMes(mes_cierre) + ' ' + anio_cierre + '.xlsx', sheet_name = 'Reserva')
print('Archivo ', fec_cierre.strftime('%m') + ' Reserva de Honorarios a Juntas a ' +  ConvertirMes(mes_cierre) + ' ' + anio_cierre, 'leído\n')

#%%
print('Leyendo archivo: Reserva de Auxilios de Funerario ' + fec_cierre.strftime('%m') + ' ' +  ConvertirMes(mes_cierre) + ' ' + anio_cierre)
Reserva_auxilios_funerarios = pd.read_excel(path_int1 + r'/' + anio_cierre + r'\Auxilios Funerarios/' + 'Reserva de Auxilios de Funerario ' + fec_cierre.strftime('%m') + ' ' +  ConvertirMes(mes_cierre) + ' ' + anio_cierre + '.xlsx', sheet_name = 'base', header = 1)
print('Archivo Reserva de Auxilios de Funerario ' + fec_cierre.strftime('%m') + ' ' +  ConvertirMes(mes_cierre) + ' ' + anio_cierre, 'leído\n')

#%%
print('Leyendo archivo: Reserva a ' + ConvertirMes(mes_cierre) + ' ' + anio_cierre, '(matemática)')
Reserva_a_mes_matematica = pd.read_excel(path1_int_final + '/Reserva a ' + ConvertirMes(mes_cierre) + ' ' + anio_cierre + '.xlsx', sheet_name = 'Matematica', header = 6)
print('Archivo: Reserva a ' + ConvertirMes(mes_cierre) + ' ' + anio_cierre, 'leído\n')

#%%
print('Leyendo archivo: Reserva a ' + ConvertirMes(mes_cierre) + ' ' + anio_cierre, '(avisados)')
Reserva_a_mes_avisados = pd.read_excel(path1_int_final + '/Reserva a ' + ConvertirMes(mes_cierre) + ' ' + anio_cierre + '.xlsx', sheet_name = 'Avisados', header = 6)
print('Archivo: Reserva a ' + ConvertirMes(mes_cierre) + ' ' + anio_cierre, 'leído\n')

#%%
print('Leyendo archivo: Circular ' + 'SIN' + anio_cierre_anterior_corto + mes_cierre_anterior)
Circular_mes_anterior = pd.read_csv(path3_int_final + '/SIN' + anio_cierre_anterior_corto + mes_cierre_anterior + '.csv', sep = ';', header = None)
print('Archivo Circular ' + 'SIN' + anio_cierre_anterior_corto + mes_cierre_anterior, '\n')

Circular_mes_anterior.columns = ['PERIODO','SINIESTRO','SECUENCIA','TIPO']
#%%
#columnas = ['PH8C01','PH8C19','PH8C20','PH8C35','PH8C36']
print('Leyendo archivo: query APH08AF0_QRY')
Query_IT_y_Asistencial = pd.read_excel(path_out1 + '/APH08AF0_QRY.xlsx')
print('Archivo: query APH08AF0_QRY leído\n')

#%%
print('Leyendo archivo: query APH06AF0_QRY')
Query_IPP = pd.read_excel(path_out1 + '/APH06AF0_QRY.xlsx', dtype = {'PH6C01':str})
print('Archivo: query APH06AF0_QRY leído\n')

#%%

print('Leyendo archivo: query ','AVI' + anio_cierre_corto + mes_cierre + '_QRY')
Query_AVI = pd.read_excel(path_out1 + '/AVI' + anio_cierre_corto + mes_cierre + '_QRY.xlsx')
print('Archivo: query ' + 'AVI' + anio_cierre_corto + mes_cierre + '_QRY leído\n')

#%%

print('Leyendo archivo: query ','MAT' + anio_cierre_corto + mes_cierre + '_QRY')
Query_MAT = pd.read_excel(path_out1 + '/MAT' + anio_cierre_corto + mes_cierre + '_QRY.xlsx')
print('Archivo: query ' + 'MAT' + anio_cierre_corto + mes_cierre + '_QRY leído\n')

#%%
columnas = [3,23,24,25,26]
print('Leyendo archivo: query ', 'AVI' + anio_cierre_anterior_corto + mes_cierre_anterior)
Query_AVI_cierre_anterior = pd.read_csv(path3_int_final + '/AVI' + anio_cierre_anterior_corto + mes_cierre_anterior + '.csv', sep = ';', encoding = 'ansi', header = None, usecols = columnas)
print('Archivo: query ' + 'AVI' + anio_cierre_anterior_corto + mes_cierre_anterior + '_QRY leído\n')

#%%
columnas = [2,16,17,18]
print('Leyendo archivo: query ','MAT' + anio_cierre_anterior_corto + mes_cierre_anterior + '_QRY')
Query_MAT_cierre_anterior = pd.read_csv(path3_int_final + '/MAT' + anio_cierre_anterior_corto + mes_cierre_anterior + '.csv', sep = ';', encoding = 'ansi', header = None, usecols = columnas)
print('Archivo: query ' + 'MAT' + anio_cierre_anterior_corto + mes_cierre_anterior + '_QRY leído\n')

#%%
columnas = ['NÚMERO DE SINIESTRO O RENTA','RESERVA TOTAL']
formatos = {'NÚMERO DE SINIESTRO O RENTA':str}
print('Leyendo archivo:', 'Reporte Reserva Matematica ARL_' + anio_cierre + '_' + ConvertirMes(mes_cierre))
Reporte_reserva_matematica_arl = pd.read_excel(path1_int_final + '/Reporte Reserva Matematica ARL_' + anio_cierre + '_' + ConvertirMes(mes_cierre) + '.xlsx', header = 2, usecols = columnas, dtype = formatos)
print('Archivo ','Reporte Reserva Matematica ARL_' + anio_cierre + '_' + ConvertirMes(mes_cierre), 'leído\n')
Reporte_reserva_matematica_arl['NÚMERO DE SINIESTRO O RENTA'] = Reporte_reserva_matematica_arl['NÚMERO DE SINIESTRO O RENTA'].str.upper().str.strip('F').str.strip('S')

#%%

columnas = ['NÚMERO DE SINIESTRO O RENTA','RESERVA TOTAL']
formatos = {'NÚMERO DE SINIESTRO O RENTA':str}
print('Leyendo archivo:', 'Reporte Reserva Matematica Avisados ARL ' + ConvertirMes(mes_cierre) + ' ' + anio_cierre)
Reporte_reserva_matematica_avisados = pd.read_excel(path1_int_final + '/Reporte Reserva Matematica Avisados ARL ' + ConvertirMes(mes_cierre) + ' ' + anio_cierre + '.xlsx', header = 2, usecols = columnas, dtype = formatos)
print('Archivo ','Reporte Reserva Matematica Avisados ARL ' + ConvertirMes(mes_cierre) + ' ' + anio_cierre, 'leído\n')
Reporte_reserva_matematica_avisados['NÚMERO DE SINIESTRO O RENTA'] = Reporte_reserva_matematica_avisados['NÚMERO DE SINIESTRO O RENTA'].str.upper().str.strip('F').str.strip('S')


#%%
Reserva_a_mes_matematica['PERIODO'] = EstandarizarFormatos(Reserva_a_mes_matematica, a = 'PERIODO')
Reserva_a_mes_avisados['PERIODO'] = EstandarizarFormatos(Reserva_a_mes_avisados, a = 'PERIODO')

Reserva_a_mes_matematica['SINIESTRO'] = Formatos_sin_sufijo(Reserva_a_mes_matematica, a = 'SINIESTRO')
Reserva_a_mes_avisados['SINIESTRO'] = Formatos_sin_sufijo(Reserva_a_mes_avisados, a = 'SINIESTRO')

Reserva_a_mes_matematica2 = Reserva_a_mes_matematica[Reserva_a_mes_matematica['PERIODO'] == (anio_cierre + mes_cierre)].copy()
Reserva_a_mes_matematica2 = Reserva_a_mes_matematica2[['TIPO', 'SINIESTRO']]
Reserva_a_mes_matematica2.loc[Reserva_a_mes_matematica2['TIPO'].astype(str).str.upper() == 'S','TIPO'] = '3'
Reserva_a_mes_matematica2.loc[Reserva_a_mes_matematica2['TIPO'].astype(str).str.upper() == 'I','TIPO'] = '2'


Reserva_a_mes_avisados2 = Reserva_a_mes_avisados[Reserva_a_mes_avisados['PERIODO'] == (anio_cierre + mes_cierre)].copy()
Reserva_a_mes_avisados2 = Reserva_a_mes_avisados2[['TIPO', 'SINIESTRO']]
Reserva_a_mes_avisados2.loc[Reserva_a_mes_avisados2['TIPO'].astype(str).str.upper() == 'S','TIPO'] = '3'
Reserva_a_mes_avisados2.loc[Reserva_a_mes_avisados2['TIPO'].astype(str).str.upper() == 'I','TIPO'] = '2'

#%%

Circular_mes_anterior['SINIESTRO'] = EstandarizarFormatos(Circular_mes_anterior, a = 'SINIESTRO')

#%%
Reserva_a_mes_avisados3 = Reserva_a_mes_avisados2[Reserva_a_mes_avisados2['SINIESTRO'].isin(Circular_mes_anterior['SINIESTRO']) ==  False]
Reserva_a_mes_avisados3['PERIODO'] = anio_cierre + mes_cierre
Reserva_a_mes_avisados3['SECUENCIA'] = 1

Reserva_a_mes_matematica3 = Reserva_a_mes_matematica2[Reserva_a_mes_matematica2['SINIESTRO'].isin(Circular_mes_anterior['SINIESTRO']) ==  False]
Reserva_a_mes_matematica3 = Reserva_a_mes_matematica3[Reserva_a_mes_matematica3['SINIESTRO'].isin(Reserva_a_mes_avisados3['SINIESTRO']) ==  False]
Reserva_a_mes_matematica3['PERIODO'] = anio_cierre + mes_cierre
Reserva_a_mes_matematica3['SECUENCIA'] = 1

#%%
Circular_mes_anterior['PERIODO'] = anio_cierre + mes_cierre
Circular_mes_anterior2 = pd.concat([Circular_mes_anterior,Reserva_a_mes_avisados3,Reserva_a_mes_matematica3]).reset_index(drop = True)

Circular_mes_anterior2 
Circular_mes_anterior2.to_csv(path_out1 + '/SIN' + anio_cierre_corto + mes_cierre + '_Prueba.csv', sep = ';', index = False, header = None)
#%%
## Agrupamos por siniestro los valores numericos sumados
reserva_IT_Asistencial['SINIESTRO'] = EstandarizarFormatos(reserva_IT_Asistencial, a = 'SINIESTRO')
reserva_IT_Asistencial = reserva_IT_Asistencial.groupby('SINIESTRO', as_index= False)['SALDO ACTUAL ASISTENCIAL','SALDO ACTUAL IT'].sum()

Query_IT_y_Asistencial['PH8C01'] = EstandarizarFormatos(Query_IT_y_Asistencial, a = 'PH8C01')

Query_IT_y_Asistencial2 = Query_IT_y_Asistencial.merge(reserva_IT_Asistencial, how = 'left', left_on = 'PH8C01', right_on = 'SINIESTRO')

#Reemplazamos las siguientes columnas
Query_IT_y_Asistencial2['PH8C19'] = Query_IT_y_Asistencial2['SALDO ACTUAL ASISTENCIAL'].copy()
Query_IT_y_Asistencial2['PH8C20'] = Query_IT_y_Asistencial2['SALDO ACTUAL IT'].copy()
Query_IT_y_Asistencial2['PH8C35'] = Query_IT_y_Asistencial2['SALDO ACTUAL ASISTENCIAL'].copy()
Query_IT_y_Asistencial2['PH8C36'] = Query_IT_y_Asistencial2['SALDO ACTUAL IT'].copy()
Query_IT_y_Asistencial2 = Query_IT_y_Asistencial2.drop(columns = ['SINIESTRO','SALDO ACTUAL ASISTENCIAL','SALDO ACTUAL IT'])

#%%

reserva_IT_Asistencial2 = reserva_IT_Asistencial.copy()
reserva_IT_Asistencial2 = reserva_IT_Asistencial2[reserva_IT_Asistencial2['SINIESTRO'].isin(Query_IT_y_Asistencial['PH8C01']) == False]
reserva_IT_Asistencial2 = reserva_IT_Asistencial2[(reserva_IT_Asistencial2['SALDO ACTUAL ASISTENCIAL'] > 0) | (reserva_IT_Asistencial2['SALDO ACTUAL IT'] > 0)]
reserva_IT_Asistencial2 = reserva_IT_Asistencial2.rename(columns = {'SINIESTRO':'PH8C01','SALDO ACTUAL ASISTENCIAL':'PH8C19','SALDO ACTUAL IT':'PH8C20'})
reserva_IT_Asistencial2['PH8C35'] = reserva_IT_Asistencial2['PH8C19'].copy()
reserva_IT_Asistencial2['PH8C36'] = reserva_IT_Asistencial2['PH8C20'].copy()

#%%
Query_IT_y_Asistencial3 = pd.concat([Query_IT_y_Asistencial2,reserva_IT_Asistencial2]).reset_index(drop = True)
Query_IT_y_Asistencial3['PH8PER'] = Query_IT_y_Asistencial3['PH8PER'].fillna(Query_IT_y_Asistencial2['PH8PER'][0])

campos_estandar = ['PH8PER', 'PH8C01', 'PH8C02', 'PH8C05', 'PH8C06',
       'PH8C07', 'PH8C08', 'PH8C09', 'PH8C10', 'PH8C12', 'PH8C13',
       'PH8C14', 'PH8C15', 'PH8C16', 'PH8C17', 'PH8C18', 'PH8C19', 'PH8C20',
       'PH8C21', 'PH8C22', 'PH8C23', 'PH8C24', 'PH8C25', 'PH8C26', 'PH8C27',
       'PH8C28', 'PH8C29', 'PH8C30', 'PH8C31', 'PH8C32', 'PH8C33', 'PH8C34',
       'PH8C35', 'PH8C36', 'PH8C37', 'PH8C38', 'PH8C39', 'PH8C40', 'PH8C41',
       'PH8C42', 'PH8C43', 'PH8C44', 'PH8C45', 'PH8C46']

for i in campos_estandar:
    Query_IT_y_Asistencial3[i] = EstandarizarFormatos(Query_IT_y_Asistencial3, a = i).fillna('0')

#%%
Query_IT_y_Asistencial3 = Query_IT_y_Asistencial3.fillna('0')
Query_IT_y_Asistencial3['PH8C03'] = Query_IT_y_Asistencial3['PH8C03'].astype(str).str.replace('\x88','')

print('Guardando archivo: ' + 'ITA' + anio_cierre_corto + mes_cierre)
Query_IT_y_Asistencial3.to_csv(path_out1 + '/ITA' + anio_cierre_corto + mes_cierre + '_Prueba.csv', sep = ';', header = False, index = False, encoding = 'ANSI')
print('Archivo: ' + 'ITA' + anio_cierre_corto + mes_cierre, ' guardado\n')

#%%
print('Actualizando valor Reserva IPP')
reserva_IPP['NUMERO DE SINIESTRO'] = EstandarizarFormatos(reserva_IPP, a = 'NUMERO DE SINIESTRO')
reserva_IPP = reserva_IPP.dropna(subset = ['NUMERO DE SINIESTRO'])

Query_IPP['PH6C01'] = EstandarizarFormatos(Query_IPP, a = 'PH6C01')

Query_IPP2 = Query_IPP.merge(reserva_IPP, how = 'left', left_on = 'PH6C01', right_on = 'NUMERO DE SINIESTRO')
del(Query_IPP2['PH6C15'], Query_IPP2['NUMERO DE SINIESTRO'])

Query_IPP2 = Query_IPP2.rename(columns = {'RESERVA IPP ACTUAL':'PH6C15'})
Query_IPP2['PH6C15'] = Query_IPP2['PH6C15'].fillna(0)
Query_IPP2['PH6C29'] = Query_IPP2['PH6C15'].fillna(0)

print('Reserva IPP actualizada, valor: $', '{:,.0f}'.format(Query_IPP2['PH6C15'].sum()))
time.sleep(3)

campos_estandar = ['PH6PER','PH6PE1','PH6C01','PH6C02','PH6C03','PH6C04','PH6C06',
                   'PH6C08','PH6C09','PH6C11','PH6C12','PH6C16','PH6C17','PH6C20',
                   'PH6C22','PH6C24','PH6C26','PH6C28','PH6C29','PH6C30','PH6C31',
                   'PH6C32','PH6C33','PH6C34','PH6C35','PH6C36','PH6C37','PH6C38',
                   'PH6C39','PH6C15']

for i in campos_estandar:
    Query_IPP2[i] = EstandarizarFormatos(Query_IPP2, a = i)
    
Query_IPP2 = Query_IPP2[Query_IPP.columns]

campos_comas = ['PH6C13','PH6C14','PH6C27']

for i in campos_comas:
    Query_IPP2[i] = Query_IPP2[i].astype(str).str.replace('.',',')
    
print('\nGuardando archivo: IPP' + anio_cierre_corto + mes_cierre)
Query_IPP2.to_csv(path_out1 + '/IPP' + anio_cierre_corto + mes_cierre + '_Prueba.csv', sep = ';', header = False, index = False, encoding = 'ANSI')
print('Archivo: IPP' + anio_cierre_corto + mes_cierre, 'guardado\n')

#%%

homologacion = {'10-No. de siniestro':'SINIESTRO','27-Mesada':'VALOR MESADA',
            '28-Número de Mesadas':'NUMERO MESADAS','06-Interés Técnico':'INTERES TECNICO',
            '11-Fecha de siniestro':'FECHA SINIESTRO'}

Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2 = Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL.copy()

Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2 = Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2.rename(columns = homologacion)

fechas = ['FECHA SINIESTRO','20-Fecha de Nacimiento','46-Fecha de Nacimiento',
          '54-Fecha de Nacimiento','62-Fecha de Nacimiento']

for i in fechas:
    Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2[i] = pd.to_datetime(Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['FECHA SINIESTRO'], format = '%Y-%m-%d')
    Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2[i] = Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2[i].dt.strftime('%Y%m%d')

Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['INTERES TECNICO'] = (Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['INTERES TECNICO']/100).astype(str).str.replace('.',',')
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['PERIODO'] = now.strftime('%Y') + now.strftime('%m')

Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['TIPO SINIESTRO'] = np.where(Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['12-Origen de la pensión'].isin([1,4]) == True, 'Invalidez','Sobrevivencia')
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['ID 1'] = np.where(Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['12-Origen de la pensión'].isin([1,4]) == True, Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['18-No. de identif.'],Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['44-No. de identif.'])
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['FECHA NACIMIENTO'] = np.where(Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['12-Origen de la pensión'].isin([1,4]) == True, Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['20-Fecha de Nacimiento'],Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['46-Fecha de Nacimiento'])
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['SEXO'] = np.where(Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['12-Origen de la pensión'].isin([1,4]) == True, Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['19-Sexo'],Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['45-Sexo'])
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['ESTADO'] = np.where(Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['12-Origen de la pensión'].isin([1,4]) == True, Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['23-Estado'],Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['49-Estado'])
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['PARENTESCO'] = np.where(Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['12-Origen de la pensión'].isin([1,4]) == True, Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['16-Parent. 01'],Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['42-Parent. 02'])
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['ID 2'] = np.where(Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['12-Origen de la pensión'].isin([1,4]) == True, Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['44-No. de identif.'],Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['52-No. de identif.'])
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['FECHA NACIMIENTO 2'] = np.where(Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['12-Origen de la pensión'].isin([1,4]) == True, Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['46-Fecha de Nacimiento'],Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['54-Fecha de Nacimiento'])
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['SEXO 2'] = np.where(Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['12-Origen de la pensión'].isin([1,4]) == True, Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['45-Sexo'],Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['53-Sexo'])
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['ESTADO 2'] = np.where(Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['12-Origen de la pensión'].isin([1,4]) == True, Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['49-Estado'],Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['57-Estado'])
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['PARENTESCO 2'] = np.where(Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['12-Origen de la pensión'].isin([1,4]) == True, Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['42-Parent. 02'],Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['50-Parent. 03'])
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['ID 3'] = np.where(Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['12-Origen de la pensión'].isin([1,4]) == True, Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['52-No. de identif.'],Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['60-No. de identif.'])
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['FECHA NACIMIENTO 3'] = np.where(Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['12-Origen de la pensión'].isin([1,4]) == True, Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['54-Fecha de Nacimiento'],Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['62-Fecha de Nacimiento'])
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['SEXO 3'] = np.where(Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['12-Origen de la pensión'].isin([1,4]) == True, Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['53-Sexo'],Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['61-Sexo'])
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['ESTADO 3'] = np.where(Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['12-Origen de la pensión'].isin([1,4]) == True, Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['57-Estado'],Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['65-Estado'])
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['PARENTESCO 3'] = np.where(Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['12-Origen de la pensión'].isin([1,4]) == True, Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['50-Parent. 03'],Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['58-Parent. 04'])
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['ID 4'] = np.where(Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['12-Origen de la pensión'].isin([1,4]) == True, Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['60-No. de identif.'], '0')
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['FECHA NACIMIENTO 4'] = np.where(Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['12-Origen de la pensión'].isin([1,4]) == True, Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['62-Fecha de Nacimiento'], '0')
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['SEXO 4'] = np.where(Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['12-Origen de la pensión'].isin([1,4]) == True, Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['61-Sexo'], '0')
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['ESTADO 4'] = np.where(Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['12-Origen de la pensión'].isin([1,4]) == True, Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['65-Estado'], '0')
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['PARENTESCO 4'] = np.where(Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['12-Origen de la pensión'].isin([1,4]) == True, Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['58-Parent. 04'], '0')
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2[['VEJEZ','INVALIDEZ','SOBREVIVENCIA','AUXILIO FUNERARIO','RESERVA','A_O','TOTAL RESERVA','-','PAR 0','RESERVA AMORTIZADA']] = '0'
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['TIPO RESERVA'] = 'Rentas'
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['CONSTITUIDA A SEP.2010'] = 'Avisados'
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['SE AMORTIZA'] = 'Si'
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2['MES INGRESO RESERVA'] = '99'

Orden = ['PERIODO','SINIESTRO','TIPO SINIESTRO','VALOR MESADA','FECHA SINIESTRO',
         'NUMERO MESADAS','INTERES TECNICO','ID 1','FECHA NACIMIENTO','SEXO','ESTADO',
         'PARENTESCO','ID 2','FECHA NACIMIENTO 2','SEXO 2','ESTADO 2','PARENTESCO 2',
         'ID 3','FECHA NACIMIENTO 3','SEXO 3','ESTADO 3','PARENTESCO 3','ID 4',
         'FECHA NACIMIENTO 4','SEXO 4','ESTADO 4','PARENTESCO 4','VEJEZ','INVALIDEZ',
         'SOBREVIVENCIA','AUXILIO FUNERARIO','RESERVA','A_O','TOTAL RESERVA','-',
         'TIPO RESERVA','CONSTITUIDA A SEP.2010','SE AMORTIZA','PAR 0','MES INGRESO RESERVA',
         'RESERVA AMORTIZADA']

Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2 = Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2[Orden]

formatos_est = ['SINIESTRO','ID 1','ID 2','ID 3','ID 4']

for i in formatos_est:
    Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2[i] = EstandarizarFormatos(Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2, i).fillna('0')

sexos = ['SEXO','SEXO 2','SEXO 3','SEXO 4']

for i in sexos:
    Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2[i] = EstandarizarFormatos(Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2, i).fillna('0')
    Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2.loc[Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2[i].astype(str) == '1', i] = 'M'
    Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2.loc[Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2[i].astype(str) == '2', i] = 'F'

estados = ['ESTADO','ESTADO 2','ESTADO 3','ESTADO 4']

for i in estados:
    Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2[i] = EstandarizarFormatos(Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2, i).fillna('0')
    Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2.loc[Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2[i].astype(str) == '1', i] = 'Válido'
    Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2.loc[Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2[i].astype(str) == '2', i] = 'Inválido'

estados = ['PARENTESCO','PARENTESCO 2','PARENTESCO 3','PARENTESCO 4']

for i in estados:
    Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2[i] = EstandarizarFormatos(Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2, i).fillna('0')
    Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2.loc[Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2[i].astype(str) == '1', i] = 'Afiliado'
    Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2.loc[Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2[i].astype(str).isin(['2','3']) == True, i] = 'Hijo'
    Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2.loc[Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2[i].astype(str) == '4', i] = 'Cónyuge'
    Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2.loc[Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2[i].astype(str) == '5', i] = 'Padre'
    Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2.loc[Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2[i].astype(str) == '6', i] = 'Madre'
    Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2.loc[Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2[i].astype(str) == '7', i] = 'Cónyuge'
    Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2.loc[Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2[i].astype(str) == '8', i] = 'Hermano'
    Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2.loc[Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2[i].astype(str) == '0', i] = '0'

#%%
print('\nGuardando archivo: MAV' + anio_cierre_corto + mes_cierre)
Formato_394_Diagnostico_de_calidad_reserva_avisados_ARL2.to_csv(path_out1 + '/MAV' + anio_cierre_corto + mes_cierre + '_Prueba.csv', sep = ';', header = False, index = False, encoding = 'ANSI')
print('Archivo: MAV' + anio_cierre_corto + mes_cierre, 'guardado\n')
#%%
print('Tiempo de duración: ', datetime.now() - now)









