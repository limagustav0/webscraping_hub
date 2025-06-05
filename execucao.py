

import time
import asyncio
import requests
from datetime import datetime
from scrape_combined_crawl4ai import process_urls, carregar_sem_dados_url
import re

async def run_combined_crawler():
    print(f'Executando scrape_combined_crawl4ai.py às {datetime.now()}')
    try:
        combined_urls = [
            "https://www.amazon.com.br/dp/B085Z5WKBB",
        ]

        # Carrega URLs que falharam anteriormente
        sem_dado = carregar_sem_dados_url()
        
        # Valida URLs para evitar concatenações ou formatos inválidos
        valid_urls = []
        for url in sem_dado + combined_urls:
            if re.match(r'^https?://', url) and ' ' not in url:  # Verifica se é uma URL válida
                valid_urls.append(url)
            else:
                print(f"URL inválida ignorada: {url}")
        
        # Remove duplicatas mantendo a ordem
        combined_urls = list(dict.fromkeys(valid_urls))
        
        await process_urls(combined_urls)
    except Exception as e:
        print(f'Erro ao executar scrape_combined_crawl4ai.py: {e}')

if __name__ == '__main__':
    asyncio.run(run_combined_crawler())


