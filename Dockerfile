FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente (carpeta src/)
COPY src/ ./src/

CMD ["python", "src/bot.py"]
