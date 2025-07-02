# Web Scraping Project

Este projeto realiza web scraping de produtos em diferentes marketplaces (Amazon, Beleza na Web e Mercado Livre) e envia os dados para uma API.

## Configuração

1. Clone o repositório
2. Instale as dependências:
```bash
pip install -r requirements.txt
playwright install chromium
```

3. Configure os secrets no GitHub:
   - Vá para Settings > Secrets and variables > Actions
   - Adicione os seguintes secrets:
     - `MELI_FTID`
     - `MELI_ORGUSERID`

## Estrutura do Projeto

- `scrape_combined_crawl4ai.py`: Script principal de scraping
- `requirements.txt`: Dependências do projeto
- `.github/workflows/scrape.yml`: Configuração do GitHub Actions

## GitHub Actions

O workflow está configurado para rodar:
- A cada 6 horas automaticamente
- Manualmente através da aba Actions

## Execução Local

Para executar localmente:

```bash
python scrape_combined_crawl4ai.py
```

## Dependências

- Python 3.x
- aiohttp
- crawl4ai
- playwright
- asyncio 

# Webscraping Hub

## Como rodar com Docker Compose

1. **Build e execute o serviço:**

```sh
docker compose up --build
```

2. **Personalize variáveis e volumes:**
- Os arquivos de cookies (`amz_auth.json`, `meli_auth.json`) devem estar na raiz do projeto.
- Para mudar a URL da API, edite o arquivo `docker-compose.yml` na seção `environment`.
- Para rodar outro script, altere o comando na seção `command` do `docker-compose.yml`.

3. **Parar o serviço:**
```sh
docker compose down
```

---

## Como rodar com Docker (manual)

1. **Build da imagem:**

```sh
docker build -t webscraping_hub .
```

2. **Execute o container:**

Se você precisa usar arquivos de cookies (ex: `amz_auth.json`, `meli_auth.json`), coloque-os na raiz do projeto e rode:

```sh
docker run -it --rm \
  -v $(pwd)/amz_auth.json:/app/amz_auth.json \
  -v $(pwd)/meli_auth.json:/app/meli_auth.json \
  webscraping_hub
```

> Se não precisar de cookies, apenas:
>
> ```sh
> docker run -it --rm webscraping_hub
> ```

3. **Variáveis de ambiente:**

Se quiser customizar a URL da API, use a flag `-e`:

```sh
docker run -it --rm -e API_URL=https://sua.api.com webscraping_hub
```

4. **Observações:**
- O Playwright já está configurado para rodar Chromium headless.
- Se quiser rodar outro script, edite o `CMD` no Dockerfile ou passe o comando manualmente:
  ```sh
  docker run -it --rm webscraping_hub python outro_script.py
  ``` 