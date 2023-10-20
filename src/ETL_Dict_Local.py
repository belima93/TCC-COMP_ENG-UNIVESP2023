import pandas as pd

descricao_dict = {
    'CAB1_E': 'CARRO COM CABINE 1',
    'Ux_CM_E': 'CARRO MOTOR SEM CABINE OPOSTA',
    'Ux_CR2_E': 'CARRO REBOQUE 2 OPOSTA',
    'CM_E': 'CARRO MOTOR SEM CABINE',
    'CR2_E': 'CARRO REBOQUE 2',
    'CR1_E': 'CARRO REBOQUE 1',
    'Ux_CAB1_E': 'CARRO COM CABINE 1 OPOSTA',
    'Ux_CR1_E': 'CARRO REBOQUE 1 OPOSTA',
    'Ux_UT_E': 'CARRO UNIDADE DO TREM OPOSTA',
    'UT_E': 'CARRO UNIDADE DO TREM',
    'CR1_P': 'CARRO REBOQUE 1',
    'CAB1_P': 'CARRO COM CABINE 1',
    'CR2_P': 'CARRO REBOQUE 2',
    'CM_P': 'CARRO MOTOR SEM CABINE',
    'Ux_CM_P': 'CARRO MOTOR SEM CABINE OPOSTA',
    'Ux_CR2_P': 'CARRO REBOQUE 2 OPOSTA',
    'Ux_CR1_P': 'CARRO REBOQUE 1 OPOSTA',
    'Ux_CAB1_P': 'CARRO COM CABINE 1 OPOSTA',
    'CAB1_E_B3': 'CARRO COM CABINE 1',
    'CAB1_E_C2': 'CARRO COM CABINE 1'
}


df = pd.read_csv('/home/b3r/Desktop/TCC-COMP_ENG-UNIVESP2023/src/base_code_local.csv', sep = ';',encoding='iso-8859-1')

# Adicione a coluna DESCRICAO com base no dicionário
df['description'] = df['location'].map(descricao_dict)

df.to_csv("/home/b3r/Desktop/TCC-COMP_ENG-UNIVESP2023/src/base_code_local.csv",index=False)

print(" Sucess in updating the location Table v2!")