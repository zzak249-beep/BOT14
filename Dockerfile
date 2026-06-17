FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# FIX v6.5: usuario no-root (mejor práctica de seguridad en contenedores)
RUN useradd -m -u 1000 botuser && chown -R botuser:botuser /app
USER botuser

EXPOSE 8080

# FIX v6.5: healthcheck para que Railway detecte si el proceso cuelga
# (aiohttp arriba pero loops en deadlock, por ejemplo)
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request,os; urllib.request.urlopen('http://127.0.0.1:' + os.environ.get('PORT','8080') + '/health', timeout=4)" || exit 1

CMD ["python", "main.py"]
