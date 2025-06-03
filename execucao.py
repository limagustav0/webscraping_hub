import time
import asyncio
import requests
from datetime import datetime
from scrape_combined_crawl4ai import process_urls


async def run_combined_crawler():

    print(f'Executando scrape_combined_crawl4ai.py às {datetime.now()}')
    try:

        combined_urls = [
            'https://www.mercadolivre.com.br/shampoo-higienizando-widi-care-a-juba-500ml-limpeza-inteligente/p/MLB19860817/s?',
            'https://www.mercadolivre.com.br/acidificante-widi-care-acidificando-a-juba-500ml/p/MLB36742897/s?',
            'https://www.mercadolivre.com.br/wella-mascara-oil-reflections-500ml/p/MLB19512787/s?'
            #'https://www.mercadolivre.com.br/oleo-finalizador-extraordinario-tratamento-sublime-nutrico-100ml-elseve/p/MLB26080271/s?pdp_filters=item_id%3AMLB3956803727',
        ]
        await process_urls(combined_urls)
    except Exception as e:
        print(f'Erro ao executar scrape_combined_crawl4ai.py: {e}')

if __name__ == '__main__':
    asyncio.run(run_combined_crawler())
