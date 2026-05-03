FROM python:3.12-slim

WORKDIR /app

# Dependencias instaladas inline — sin COPY requirements.txt
RUN pip install --no-cache-dir aiohttp==3.9.5 numpy==1.26.4

# Copiar el código fuente
COPY src/ ./src/

CMD ["python", "src/bot.py"]
