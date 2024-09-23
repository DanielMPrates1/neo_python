import aiohttp
import asyncio
import json
import pandas as pd
from parsel import Selector
import re
import nest_asyncio

# Permite que asyncio.run seja chamado em um loop de eventos existente

def executa_compara_frete_minha_casa_solar(product_ids):
   
    # Importa base de Ceps
    ceps = pd.read_csv(r"C:\Users\Daniel\Desktop\comparativo_frete\postcode.csv")
    postcodes_df = pd.DataFrame(ceps)

    # Chama a função assíncrona
    mcs = asyncio.run(get_shipping_options(postcodes_df, product_ids))
    print(mcs)
    return mcs


nest_asyncio.apply()

async def fetch_shipping_options(session, url, payload):
    """
    Faz uma requisição assíncrona para obter opções de envio de um produto para um CEP específico.

    Args:
        session (aiohttp.ClientSession): Sessão assíncrona do cliente para fazer a requisição.
        url (str): URL para a API de envio.
        payload (dict): Dados a serem enviados no corpo da requisição.

    Returns:
        list: Lista de opções de envio, cada uma contendo a transportadora, custo e prazo.
    """
    async with session.post(url, json=payload) as response:
        if response.status == 200:
            text = await response.text()
            selector = Selector(text=text)
            options = selector.css('li.menu-item.frete')
            shipping_options = []

            for option in options:
                transportadora = option.css('strong::text').get()
                detalhes = option.css('span::text').get()
                
                # Extrai apenas os números da coluna de prazo
                prazo_match = re.search(r'(\d+) dias úteis', detalhes)
                prazo = int(prazo_match.group(1)) if prazo_match else None
                
                valor_match = re.search(r'R\$ (\d+,\d+)', detalhes)
                valor = valor_match.group(1) if valor_match else None
                
                shipping_options.append({
                    'opcao': transportadora.replace(":", ""),
                    'prazo': prazo,
                    'valor': valor,
                })

            return shipping_options
        else:
            print(f"Erro ao obter dados para o CEP: Status {response.status}")
            return []

async def get_shipping_options(postcodes_df, product_ids):
    """
    Obtém opções de envio para uma lista de CEPs de forma assíncrona.

    Args:
        postcodes_df (pd.DataFrame): DataFrame contendo os CEPs para os quais as opções de envio serão obtidas.
        product_ids (list): Lista de product_ids a serem utilizados nas requisições.

    Returns:
        pd.DataFrame: DataFrame contendo todas as opções de envio obtidas.
    """
    url = "https://www.minhacasasolar.com.br/snippet"
    all_shipping_options = []

    async with aiohttp.ClientSession() as session:
        tasks = []

        # Itera sobre os product_ids e os CEPs
        for product_id in product_ids:
            for index, row in postcodes_df.iterrows():
                postcode = row['CEP']
                region = row['UF']

                payload = {
                    "fileName": "product_shipping_quotes_snippet.html",
                    "queryName": "SnippetQueries/shipping_quotes.graphql",
                    "variables": {
                        "cep": row['CEP'],
                        "productVariantId": product_id,
                    }
                }

                task = fetch_shipping_options(session, url, payload)
                tasks.append(task)

        results = await asyncio.gather(*tasks)

        for index, shipping_options in enumerate(results):
            product_id_index = index // len(postcodes_df)  # Calcula qual product_id está sendo processado
            product_id = product_ids[product_id_index]
            postcode = postcodes_df.iloc[index % len(postcodes_df)]['CEP']
            region = postcodes_df.iloc[index % len(postcodes_df)]['UF']

            for option in shipping_options:
                all_shipping_options.append({
                    'CEP': postcode,
                    'UF': region,
                    'Transportadora': option['opcao'],
                    'Prazo': option['prazo'],
                    'Custo': option['valor'],
                    'Empresa':'MCS',
                    'SKU': product_id,
                })

    neo = pd.DataFrame(all_shipping_options)
    return neo

# Teste com uma lista de product_ids
#product_ids = [264790, 265164]
#teste = executa_compara_frete_minha_casa_solar(product_ids)
#teste.head(5)
