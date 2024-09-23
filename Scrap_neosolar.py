import aiohttp
import asyncio
import pandas as pd
from parsel import Selector
import re
import nest_asyncio
import time

# Permite rodar o asyncio no Jupyter se necessário
nest_asyncio.apply()

def executa_compara_frete_neosolar(product_ids):
    """
    Executa a função de comparação de frete para múltiplos product_id.

    Args:
        product_ids (list): Lista de product_id para os quais as requisições serão feitas.
    """
    # Importa base de Ceps
    ceps = pd.read_csv(r"C:\Users\Daniel\Desktop\comparativo_frete\postcode.csv")
    postcodes_df = pd.DataFrame(ceps)

    # Chama a função assíncrona
    neosolar = asyncio.run(get_shipping_options(postcodes_df, product_ids))
    print(neosolar)
    return neosolar

async def fetch_shipping_options(session, url, params):
    """
    Faz uma requisição assíncrona para obter opções de envio de um produto para um CEP específico.

    Args:
        session (aiohttp.ClientSession): Sessão assíncrona do cliente para fazer a requisição.
        url (str): URL para a API de envio.
        params (dict): Parâmetros da requisição, incluindo ID do produto, país e CEP.

    Returns:
        list: Lista de opções de envio, cada uma contendo a transportadora, custo e prazo.
    """
    async with session.get(url, params=params) as response:
        if response.status == 200:
            text = await response.text()
            selector = Selector(text=text)
            rows = selector.css('table.product-shipping-table tr')
            shipping_options = []

            for row in rows[1:]:  # Pula a linha do cabeçalho
                option = row.css('td:nth-child(1)::text').get()
                prazo_text = row.css('td:nth-child(2)::text').get()
                valor = row.css('td:nth-child(3) .price::text').get()
                
                # Extrai apenas os números da coluna de prazo
                prazo_match = re.search(r'(\d+)', prazo_text)
                prazo = int(prazo_match.group(1)) if prazo_match else None

                shipping_options.append({
                    'Transportadora': option,
                    'Custo': valor,
                    'Prazo': prazo,
                })

            return shipping_options
        else:
            print(f"Erro ao obter dados para o CEP {params['postcode']}: Status {response.status}")
            return []

async def get_shipping_options(postcodes_df, product_ids):
    """
    Obtém opções de envio para uma lista de CEPs e product_ids de forma assíncrona.

    Args:
        postcodes_df (pd.DataFrame): DataFrame contendo os CEPs e regiões para os quais as opções de envio serão obtidas.
        product_ids (list): Lista de product_id para os quais as requisições serão feitas.

    Returns:
        pd.DataFrame: DataFrame contendo todas as opções de envio obtidas.
    """
    url = 'https://www.neosolar.com.br/intelipost/product/shipping/'
    country = 'BR'

    all_shipping_options = []

    async with aiohttp.ClientSession() as session:
        tasks = []
        for product_id in product_ids:
            for index, row in postcodes_df.iterrows():
                postcode = row['CEP']
                region = row['UF']
                
                params = {
                    'product': product_id,
                    'country': country,
                    'postcode': postcode
                }

                task = fetch_shipping_options(session, url, params)
                tasks.append(task)

        results = await asyncio.gather(*tasks)

        # Percorre os resultados e associa com o CEP, UF e product_id correspondente
        task_index = 0
        for product_id in product_ids:
            for index in range(len(postcodes_df)):
                postcode = postcodes_df.iloc[index]['CEP']
                region = postcodes_df.iloc[index]['UF']
                shipping_options = results[task_index]
                task_index += 1

                for option in shipping_options:
                    all_shipping_options.append({
                        'CEP': postcode,
                        'UF': region,
                        'Transportadora': option['Transportadora'],
                        'Custo': option['Custo'],
                        'Prazo': option['Prazo'],
                        'SKU': product_id,
                        'Empresa': 'Neo',
                    })
            time.sleep(10)

    neo = pd.DataFrame(all_shipping_options)
    return neo

# Exemplo de como chamar a função com uma lista de product_ids
#executa_compara_frete_neosolar(['480', '13019'])
