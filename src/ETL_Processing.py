import pandas as pd
import datetime

df = pd.read_csv('/home/b3r/Desktop/TCC-COMP_ENG-UNIVESP2023/src/base_1.csv', sep = ';',encoding='iso-8859-1')
df['DATA/HORA'] = pd.to_datetime(df['DATA/HORA'],dayfirst=True)
df = df[['DATA/HORA','ID_ALARME','ESTADO','SEVERIDADE','CÓDIGO','DESCRIÇÃO','LOCALIZAÇÃO']]
df['ID_ALARME'] = df['ID_ALARME'].astype(str)
df['ESTADO'] = df['ESTADO'].astype(bool)
df.to_csv("/home/b3r/Desktop/TCC-COMP_ENG-UNIVESP2023/src/base_ETL_True.csv",index=False)

print("Sucessful Data Processing!")

df_falhas = df[df['ESTADO']==True]
df_falhas = df_falhas[['CÓDIGO','DESCRIÇÃO']]
df_falhas = df_falhas.drop_duplicates()
df_falhas = df_falhas.dropna()
df_falhas.to_csv("/home/b3r/Desktop/TCC-COMP_ENG-UNIVESP2023/src/base_code_failure.csv",index=False)

print(" Sucess in updating the Failure Table !")