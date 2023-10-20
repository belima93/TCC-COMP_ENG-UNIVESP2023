import pandas as pd
import datetime

df = pd.read_csv('/home/b3r/Desktop/TCC-COMP_ENG-UNIVESP2023/src/base_1.csv', sep = ';',encoding='iso-8859-1')
df['DATA/HORA'] = pd.to_datetime(df['DATA/HORA'],dayfirst=True)
df = df[['DATA/HORA','ID_ALARME','ESTADO','SEVERIDADE','CÓDIGO','DESCRIÇÃO','LOCALIZAÇÃO']]
df['ID_ALARME'] = df['ID_ALARME'].astype(str)
df['ESTADO'] = df['ESTADO'].astype(bool)
df = df.dropna(subset=['DATA/HORA'])
df = df.rename(columns = {
    df.columns[0]: 'date_time',
    df.columns[1]: 'id_alarme',
    df.columns[2]: 'status',
    df.columns[3]: 'severity',
    df.columns[4]: 'code_fail',
    df.columns[5]: 'description',
    df.columns[6]: 'location'
})

df.to_csv("/home/b3r/Desktop/TCC-COMP_ENG-UNIVESP2023/src/base_ETL_True.csv",index=False)

print("Sucess in updating Data Processing!")

df_failures = df[df['status']==True]
df_failures = df_failures[['code_fail','description']]
df_failures = df_failures.drop_duplicates()
df_failures = df_failures.dropna()
df_failures.to_csv("/home/b3r/Desktop/TCC-COMP_ENG-UNIVESP2023/src/base_code_failure.csv",index=False)

print(" Sucess in updating the Failure Table !")

df_local = df['location'].drop_duplicates().dropna()
df_local.to_csv("/home/b3r/Desktop/TCC-COMP_ENG-UNIVESP2023/src/base_code_local.csv",index=False)

print(" Sucess in updating the location Table !")