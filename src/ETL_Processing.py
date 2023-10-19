import pandas as pd
import datetime

df = pd.read_csv('/home/b3r/Desktop/TCC-COMP_ENG-UNIVESP2023/src/base_1.csv', sep = ';',encoding='iso-8859-1')
df['DATA/HORA'] = pd.to_datetime(df['DATA/HORA'],dayfirst=True)
df = df[['DATA/HORA','ID_ALARME','ESTADO','SEVERIDADE','CÓDIGO','DESCRIÇÃO','LOCALIZAÇÃO']]
df['ID_ALARME'] = df['ID_ALARME'].astype(str)
df['ESTADO'] = df['ESTADO'].astype(bool)
df = df.dropna(subset=['DATA/HORA'])
df.to_csv("/home/b3r/Desktop/TCC-COMP_ENG-UNIVESP2023/src/base_ETL_True.csv",index=False)

print("Sucess in updating Data Processing!")

df_failures = df[df['ESTADO']==True]
df_failures = df_failures[['CÓDIGO','DESCRIÇÃO']]
df_failures = df_failures.drop_duplicates()
df_failures = df_failures.dropna()
df_failures.to_csv("/home/b3r/Desktop/TCC-COMP_ENG-UNIVESP2023/src/base_code_failure.csv",index=False)

print(" Sucess in updating the Failure Table !")

df_local = df['LOCALIZAÇÃO'].drop_duplicates().dropna()
df_local.to_csv("/home/b3r/Desktop/TCC-COMP_ENG-UNIVESP2023/src/base_code_local.csv",index=False)

print(" Sucess in updating the location Table !")