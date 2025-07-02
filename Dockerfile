# Dockerfile para webscraping_hub
FROM python:3.12-slim

# Instale dependências do sistema para Playwright + Chromium
RUN apt-get update && \
    apt-get install -y wget gnupg2 libnss3 libatk-bridge2.0-0 libgtk-3-0 libxss1 libasound2 libgbm1 libxshmfence1 libxcomposite1 libxrandr2 libu2f-udev libdrm2 libxdamage1 libxfixes3 libxext6 libx11-6 libx11-xcb1 libxcb1 fonts-liberation libappindicator3-1 xdg-utils git && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copie os arquivos do projeto
COPY . /app

# Instale dependências Python
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Instale Playwright e Chromium
RUN pip install playwright && playwright install --with-deps chromium

# Comando padrão (ajuste se quiser rodar outro script)
CMD ["python", "scrape_combined_crawl4ai.py"] 