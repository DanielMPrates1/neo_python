import aiohttp
import asyncio
import pandas as pd
import parsel
import nest_asyncio
import re
from datetime import datetime


# Permite que asyncio.run seja chamado em um loop de eventos existente
nest_asyncio.apply()

# Define o número máximo de tarefas simultâneas
MAX_CONCURRENT_REQUESTS = 2

#------------------------------------------------------------------------------------------------------------TRATA O DATAFRAME-------------------------------------------------------------------------
def processar_dataframe(df):
    def extrair_e_calcular_diferenca(prazo):
        hoje = datetime.now()
        
        # Verifica se a string contém 'até', indicando um intervalo de datas
        if isinstance(prazo, str) and 'até' in prazo:
            datas = re.findall(r'\d{2}/\d{2}/\d{4}', prazo)
            if datas:
                ultima_data = max(datas, key=lambda d: datetime.strptime(d, '%d/%m/%Y'))
        elif isinstance(prazo, str):
            datas = re.findall(r'\d{2}/\d{2}/\d{4}', prazo)
            if datas:
                ultima_data = datas[0]  # Apenas uma data
            else:
                return None, None  # Caso não haja data
        else:
            return None, None  # Caso 'prazo' não seja uma string válida
        
        ultima_data = datetime.strptime(ultima_data, '%d/%m/%Y')
        diferenca = (ultima_data - hoje).days
        
        return ultima_data.strftime('%d/%m/%Y'), diferenca
    
    # Garantir que df seja uma cópia para evitar o SettingWithCopyWarning
    df = df[df['Custo_ET'] != 'FRETE GRÁTIS'].copy()
    
    # Aplicar a função corretamente e garantir que a extração seja feita corretamente
    df[['Última Data', 'Prazo']] = df['Prazo de Entrega'].apply(
        lambda x: pd.Series(extrair_e_calcular_diferenca(x))
    )
    
    # Dropar a coluna antiga e renomear a nova
    df = df.drop(columns=['Prazo de Entrega', 'Última Data'])
    df = df.rename(columns={"Prazo": "Prazo_ET"})
    
    return df



#-----------------------------------------------------------------------------------------------------------FAZ O SCRAPING-------------------------------------------------------------------------

async def fetch_shipping_options(session, url, params):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    async with session.get(url, params=params, headers=headers) as response:
        if response.status == 200:
            text = await response.text()
            selector = parsel.Selector(text=text)

            shipping_options = []
            rows = selector.css('table.tablePage')[1].css('tbody > tr')[1:]  # Pulando a linha do cabeçalho

            for row in rows:
                method = row.css('td:nth-child(2)::text').get().strip()
                cost = row.css('td:nth-child(3) strong::text').get().strip()
                delivery_time = row.css('td:nth-child(4)::text').get().strip()
                shipping_options.append({
                    'Método de Envio': method,
                    'Custo': cost,
                    'Prazo de Entrega': delivery_time
                })

            return shipping_options
        else:
            return []

async def get_shipping_options(postcodes_df):
    url = "https://www.energiatotal.com.br/mvc/store/product/shipping/"
    product_id = '63'

    all_shipping_options = []
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async def fetch_with_semaphore(params):
        async with semaphore:
            async with aiohttp.ClientSession() as session:
                return await fetch_shipping_options(session, url, params)

    tasks = []
    for index, row in postcodes_df.iterrows():
        cep = row['CEP']
        region = row['UF']
        params = {
            'simular': 'ok',
            'cep1': cep,
            'quantidade': '1',
            'id_produto': product_id,
        }
        task = fetch_with_semaphore(params)
        tasks.append(task)

    results = await asyncio.gather(*tasks)

    for index, shipping_options in enumerate(results):
        if shipping_options:  # Verifica se a lista não está vazia
            cep = postcodes_df.iloc[index]['CEP']
            region = postcodes_df.iloc[index]['UF']
            #print(f"CEP {cep} tem {len(shipping_options)} opções de envio")  # Depuração

            for option in shipping_options:
                all_shipping_options.append({
                    'CEP': cep,
                    'Região': region,
                    'Transportadora_ET': option['Método de Envio'],
                    'Custo_ET': option['Custo'],
                    'Prazo de Entrega': option['Prazo de Entrega'],
                    'Empresa_ET' : 'Energie_Total',
                    'SKU': product_id
                })

    df_bruto = pd.DataFrame(all_shipping_options)
    df_tratado =  processar_dataframe(df_bruto)
    return df_tratado


    
#---------------------------------------------------------------------------------------------------CHAMA A FUNÇÃO PRICIPAL E RETORNA O DATAFRAME TRATADO-----------------------------
def executa_compara_frete_energie_solar():
    # Importa base de Ceps
    ceps = pd.read_csv(r"C:\Users\Daniel\Desktop\comparativo_frete\postcode.csv")
    postcodes_df = pd.DataFrame(ceps)
    Energie_total = asyncio.run(get_shipping_options(postcodes_df))
    
    return Energie_total


#teste = executa_compara_frete_energie_solar()
#teste.head(5)