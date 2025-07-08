FROM python:3.12.10-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt \
    && python -m playwright install --with-deps

COPY . .

CMD ["python", "scrape_combined_crawl4ai.py"] 