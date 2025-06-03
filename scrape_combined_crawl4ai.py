import asyncio
import re
import json
from datetime import datetime
import aiohttp
from crawl4ai import AsyncWebCrawler
import os
from playwright.async_api import async_playwright, Error as PlaywrightError
from playwright.sync_api import sync_playwright
import re
import json
from pprint import pprint

def extract_data_from_markdown_amazon(markdown):
    """Extrai SKU, descrição, loja, preço, review, imagem e outros dados do Markdown (Amazon)."""
    lojas = []

    # Extrai SKU (ASIN) da URL no Markdown
    sku_pattern = r'https://www\.amazon\.com\.br/.*/dp/([A-Z0-9]{10})'
    sku_match = re.search(sku_pattern, markdown)
    sku = sku_match.group(1) if sku_match else None
    if not sku:
        print('SKU não encontrado no Markdown (Amazon)')
        return []

    # Extrai descrição (nome do produto)
    desc_pattern = r'Este item:?\s*([^\n]+?)\s*(?=R\$|\n)'
    desc_match = re.search(desc_pattern, markdown, re.MULTILINE)
    descricao = desc_match.group(1).strip() if desc_match else None
    # Fallback: usa o título da imagem
    if not descricao or 'devolvido' in descricao.lower():
        img_desc_pattern = (
            r'!\[([^\]]+?)\]\(https://images-na\.ssl-images-amazon\.com'
        )
        img_desc_match = re.search(img_desc_pattern, markdown)
        descricao = (
            img_desc_match.group(1).strip()
            if img_desc_match
            else 'Descrição não encontrada'
        )
    print(f'Descrição capturada (Amazon): {descricao!r}')

    # Extrai loja (quem envia)
    loja_pattern = (
        r'Enviado de e vendido por\s*(Amazon\.com\.br|[^\n.]+?)(?:\.|\n|$)'
    )
    loja_match = re.search(loja_pattern, markdown)
    nome_loja = loja_match.group(1).strip() if loja_match else 'Amazon.com.br'

    # Extrai preço (prioriza priceAmount do JSON)
    preco_final = 0.0
    json_preco_pattern = r'"priceAmount":([\d.]+)'
    json_preco_match = re.search(json_preco_pattern, markdown)
    if json_preco_match:
        try:
            preco_final = float(json_preco_match.group(1))
        except ValueError:
            print(
                f'Erro ao converter preço do JSON (Amazon): {json_preco_match.group(1)}'
            )
    else:
        # Fallback: usa a regex original
        preco_pattern = r'R\$([\d,.]+?)(?=\s*\(R\$\s*[\d,.]+/Mililitros\)|$)'
        preco_match = re.search(preco_pattern, markdown)
        if preco_match:
            preco_final_str = (
                preco_match.group(1).replace('.', '').replace(',', '.')
            )
            try:
                preco_final = float(preco_final_str)
            except ValueError:
                print(f'Erro ao converter preço (Amazon): {preco_final_str}')

    # Extrai review (fallback)
    review_pattern = r'(\d+\.\d+)\s*(?:de\s*5\s*estrelas|out\s*of\s*5\s*stars)'
    review_match = re.search(review_pattern, markdown)
    review = float(review_match.group(1)) if review_match else 4.5

    # Extrai imagem
    img_pattern = r'!\[.*?\]\((https://images-na\.ssl-images-amazon\.com/images/I/.*?\.jpg)\)'
    img_match = re.search(img_pattern, markdown)
    imagem = img_match.group(1) if img_match else 'Imagem não encontrada'

    # Monta o dicionário da loja
    key_loja = nome_loja.lower().replace(' ', '_').replace('.', '')
    loja = {
        'sku': sku,
        'loja': nome_loja,
        'preco_final': preco_final,
        'data_hora': datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'marketplace': 'Amazon',
        'change_price': 0,
        'key_loja': key_loja,
        'key_sku': f'{key_loja}_{sku}',
        'descricao': descricao,
        'review': review,
        'imagem': imagem,
        'status': 'ativo',
    }
    lojas.append(loja)

    return lojas

def extract_data_from_markdown_beleza(markdown):
    """Extrai SKU, descrição, review, imagem e dados de lojas do Markdown (Beleza na Web)."""
    lojas = []

    # Extrai SKU
    sku_pattern = r'\*\*Cod:\*\* (MP\d+|\d+)'
    sku_match = re.search(sku_pattern, markdown)
    sku = sku_match.group(1) if sku_match else None
    if not sku:
        print('SKU não encontrado no Markdown (Beleza na Web)')
        return []

    # Extrai descrição
    desc_pattern = r'\[Voltar para a página do produto\]\(https://www\.belezanaweb\.com\.br/(.+?)\)'
    desc_match = re.search(desc_pattern, markdown)
    if desc_match:
        url_text = desc_match.group(1)
        descricao = ' '.join(word.capitalize() for word in url_text.split('-'))
        descricao = descricao.replace('Condicionador ', 'Condicionador - ')
    else:
        descricao = 'Descrição não encontrada'
    print(f'Descrição capturada (Beleza na Web): {descricao!r}')

    # Extrai review
    review_pattern = r'Review[:\s]*(\d+[\.,]\d+|\d+)'
    review_match = re.search(review_pattern, markdown)
    review = (
        float(review_match.group(1).replace(',', '.')) if review_match else 4.5
    )

    # Extrai imagem
    img_pattern_with_desc = r'!\[.*?\]\((https://res\.cloudinary\.com/beleza-na-web/image/upload/.*?/v1/imagens/product/.*?/.*?\.(?:png|jpg))\)'
    img_match_with_desc = re.search(img_pattern_with_desc, markdown)
    imagem = (
        img_match_with_desc.group(1)
        if img_match_with_desc
        else 'Imagem não encontrada'
    )
    if imagem == 'Imagem não encontrada':
        img_pattern_empty = r'!\[\]\((https?://[^\s)]+)\)'
        img_matches_empty = re.findall(img_pattern_empty, markdown)
        imagem = (
            img_matches_empty[0]
            if img_matches_empty
            else 'Imagem não encontrada'
        )

    # Extrai lojas e preços
    loja_pattern = r'Vendido por \*\*(.*?)\*\* Entregue por Beleza na Web'
    preco_com_desconto_pattern = r'-[\d]+%.*?\nR\$ ([\d,\.]+)'
    preco_venda_pattern = r'(?<!De )R\$ ([\d,\.]+)(?!\s*3x)'
    blocos = re.split(
        r'(?=Vendido por \*\*.*?\*\* Entregue por Beleza na Web)', markdown
    )
    for bloco in blocos:
        if 'Vendido por' not in bloco:
            continue
        loja_match = re.search(loja_pattern, bloco)
        preco_com_desconto_match = re.search(preco_com_desconto_pattern, bloco)
        preco_venda_match = re.search(preco_venda_pattern, bloco)
        nome_loja = loja_match.group(1) if loja_match else 'Beleza na Web'
        if preco_com_desconto_match:
            preco_final_str = preco_com_desconto_match.group(1)
            preco_final_str = preco_final_str.replace('.', '').replace(
                ',', '.'
            )
            preco_final = float(preco_final_str)
        elif preco_venda_match:
            preco_final_str = preco_venda_match.group(1)
            preco_final_str = preco_final_str.replace('.', '').replace(
                ',', '.'
            )
            preco_final = float(preco_final_str)
        else:
            preco_final = 0.0
        key_loja = nome_loja.lower().replace(' ', '')
        loja = {
            'sku': sku,
            'loja': nome_loja,
            'preco_final': preco_final,
            'data_hora': datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'marketplace': 'Beleza na Web',
            'change_price': 0,
            'key_loja': key_loja,
            'key_sku': f'{key_loja}_{sku}',
            'descricao': descricao,
            'review': review,
            'imagem': imagem,
            'status': 'ativo',
        }
        lojas.append(loja)

    return lojas

async def extract_data_from_meli(url: str) -> list:
    """
    Extrai dados de produtos do Mercado Livre e retorna uma lista de vendedores usando Playwright Async API.
    
    Args:
        url: URL da página do produto
        
    Returns:
        list: Lista de dicionários com dados dos vendedores, formatada como Beleza na Web
    """
    lojas = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            context = await browser.new_context()
            
            # Adiciona cookies do ambiente
            cookies = [
                {
                    "name": "ftid",
                    "value": os.environ.get("MELI_FTID", ""),
                    "domain": ".mercadolivre.com.br",
                    "path": "/"
                },
                {
                    "name": "orguserid",
                    "value": os.environ.get("MELI_ORGUSERID", ""),
                    "domain": ".mercadolivre.com.br",
                    "path": "/"
                }
            ]
            await context.add_cookies(cookies)
            
            page = await context.new_page()
            await page.goto(url, timeout=60000)  # Timeout de 60 segundos
            
            # Extrair SKU da URL
            sku = None
            try:
                # Updated regex to match /p/MLB12345678/ or item_id%3AMLB12345678
                match = re.search(r'(?:/p/|item_id%3A)(MLB\d+)', url)
                sku = match.group(1) if match else None
            except Exception as e:
                print(f"Erro ao extrair SKU: {e}")
            if not sku:
                print(f'SKU não encontrado na URL: {url}')
            
            # Extrair descrição
            descricao = None
            try:
                descricao = await page.locator('.ui-pdp-title').inner_text()
            except Exception as e:
                print(f"Erro ao extrair descrição: {e}")
                descricao = 'Descrição não encontrada'
            
            # Extrair imagem
            imagem = None
            try:
                imagem = await page.locator(
                    'xpath=//*[@id="root-app"]/div/div[1]/div/div/div/div/a/figure/img'
                ).get_attribute('src')
            except Exception as e:
                print(f"Erro ao extrair imagem: {e}")
            imagem = imagem if imagem else 'Imagem não encontrada'
            
            # Extrair review
            review = 4.5  # Fallback value to match Beleza na Web
            try:
                review_text = await page.locator('.ui-pdp-reviews__rating__summary__average').inner_text()
                review_match = re.search(r'(\d+\.\d+)', review_text)
                if review_match:
                    review = float(review_match.group(1))
            except Exception as e:
                print(f"Erro ao extrair review: {e}")
            
            # Extrair dados do melidata
            try:
                html_content = await page.content()
                pattern = r'melidata\("add", "event_data", ({.*?})\);'
                match = re.search(pattern, html_content, re.DOTALL)
                
                if match:
                    event_data = json.loads(match.group(1))
                    items = event_data.get('items', [])
                    
                    for item in items:
                        nome_loja = item.get('seller_name', 'Mercado Livre')
                        key_loja = nome_loja.lower().replace(' ', '')  # Match Beleza na Web
                        
                        seller = {
                            'sku': sku if sku else 'SKU não encontrado',
                            'loja': nome_loja,
                            'preco_final': float(item.get('price', 0.0)),
                            'data_hora': datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
                            'marketplace': 'Mercado Livre',
                            'change_price': 0,
                            'key_loja': key_loja,
                            'key_sku': f'{key_loja}_{sku}' if key_loja and sku else None,
                            'descricao': descricao,
                            'review': review,
                            'imagem': imagem,
                            'status': 'ativo'
                        }
                        lojas.append(seller)
                    
                    # Salvar dados em JSON
                    with open('sellers_melidata.json', 'w', encoding='utf-8') as f:
                        json.dump(lojas, f, ensure_ascii=False, indent=2)
            except Exception as e:
                print(f"Erro ao extrair dados do melidata: {e}")
            
            # Salvar estado da sessão
            try:
                await context.storage_state(path='meli.json')
            except Exception as e:
                print(f"Erro ao salvar storage state: {e}")
                
        except Exception as e:
            print(f"Erro geral no processamento da URL {url}: {e}")
        finally:
            await context.close()
            await browser.close()
    
    return lojas

async def crawl_url(crawler, url, max_retries=3):
    """Extrai dados de uma URL usando Crawl4AI ou Playwright (para Mercado Livre) com re-tentativas."""
    for attempt in range(max_retries):
        try:
            print(
                f'Extraindo dados da URL: {url} (Tentativa {attempt + 1}/{max_retries})'
            )
            if 'mercadolivre' in url.lower():
                lojas = await extract_data_from_meli(url)
            else:
                result = await crawler.arun(
                    url=url,
                    timeout=180,
                    js_enabled=True,
                    bypass_cache=True,
                    headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                    },
                )
                markdown_content = result.markdown
                print('Markdown gerado:')

                if 'amazon' in url.lower():
                    lojas = extract_data_from_markdown_amazon(markdown_content)
                elif 'belezanaweb' in url.lower():
                    lojas = extract_data_from_markdown_beleza(markdown_content)
                else:
                    print(f'URL não reconhecida: {url}')
                    return []

            if not lojas:
                print(f'Sem dados ou SKU não encontrado para {url}')
                return []
            return lojas
        except PlaywrightError as e:
            print(f'Erro do Playwright na tentativa {attempt + 1}: {e}')
            if attempt < max_retries - 1:
                print('Tentando novamente...')
                await asyncio.sleep(2)
            else:
                print(
                    f'Erro ao crawlear a URL {url} após {max_retries} tentativas: {e}'
                )
                return []
        except Exception as e:
            print(
                f'Erro ao crawlear a URL {url} na tentativa {attempt + 1}: {e}'
            )
            return []

async def send_to_api(data):
    """Envia os dados dos vendedores para a API (POST)."""
    api_url = 'https://www.price.kamico.com.br/api/products'
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(api_url, json=data, headers={'Content-Type': 'application/json'}) as response:
                print(f"Status da resposta (POST): {response.status}")
                return response.status
        except Exception as e:
            print(f"Erro ao enviar dados para a API (POST): {e}")
            return None

async def update_to_api(data):
    """Atualiza os dados dos vendedores na API (PUT)."""
    api_url = 'http://34.233.35.91:8000/api/products'
    async with aiohttp.ClientSession() as session:
        try:
            async with session.put(
                api_url,
                json=data,
                headers={'Content-Type': 'application/json'},
            ) as response:
                print(f'Status da resposta (PUT): {response.status}')
                return response.status
        except Exception as e:
            print(f'Erro ao enviar dados para a API (PUT): {e}')
            return None

def save_sem_dados_urls(sem_dados):
    """Salva URLs sem dados em um arquivo JSON."""
    try:
        with open('sem_dados_urls.json', 'w', encoding='utf-8') as f:
            json.dump(sem_dados, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f'Erro ao salvar sem_dados_urls.json: {e}')

def carregar_sem_dados_url():
    """Carrega URLs que falharam em execuções anteriores de um arquivo JSON."""
    try:
        if os.path.exists('sem_dados_urls.json'):
            with open('sem_dados_urls.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        return []
    except Exception as e:
        print(f'Erro ao carregar sem_dados_urls.json: {e}')
        return []

async def process_urls(urls):
    """Processa URLs de Amazon, Beleza na Web e Mercado Livre e envia os itens para a API."""
    sem_dado = carregar_sem_dados_url()
    combined_urls = list(dict.fromkeys(sem_dado + urls))
    total_urls = len(combined_urls)
    processed_count = 0
    sem_dados = []
    successful_urls = 0

    print(f'Total de URLs a processar: {total_urls} (incluindo {len(sem_dado)} URLs de execuções anteriores)')
    async with AsyncWebCrawler(verbose=True) as crawler:
        for url in combined_urls:
            processed_count += 1
            print(f'Processado {processed_count}/{total_urls} URLs')
            result = await crawl_url(crawler, url)
            print('Dados extraídos:')
            pprint(result, indent=2)  # Use pprint for structured output
            if result:
                post_status = await send_to_api(result)
                if post_status in (200, 201):
                    print(f'Dados salvos com sucesso para {url}, POST concluído.')
                    successful_urls += 1
                elif post_status == 400:
                    put_status = await update_to_api(result)
                    if put_status != 202:
                        print(f'Falha ao atualizar dados de {url} (Status: {put_status})')
                        sem_dados.append(url)
                    else:
                        print(f'Dados atualizados com sucesso para {url}, PUT concluído.')
                        successful_urls += 1
                else:
                    print(f'Falha ao salvar dados de {url} (Status: {post_status})')
                    sem_dados.append(url)
            else:
                print(f'Sem dados para {url}, marcando para lista de URLs sem dados')
                sem_dados.append(url)

    save_sem_dados_urls(sem_dados)
    print(f'Processamento concluído: {processed_count}/{total_urls} URLs processadas')
    print(f'Resultados: {successful_urls} URLs bem-sucedidas, {len(sem_dados)} URLs falharam, {len(sem_dados)} URLs sem dados')

if __name__ == "__main__":
    url = "https://www.mercadolivre.com.br/shampoo-higienizando-widi-care-a-juba-500ml-limpeza-inteligente/p/MLB19860817/s?"
    result = asyncio.run(extract_data_from_meli(url))
    if result:
        print("\nDados dos vendedores extraídos:")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        print(f"\nTotal de vendedores: {len(result)}")
    else:
        print("Nenhum dado foi extraído")