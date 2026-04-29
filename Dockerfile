FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ make libffi-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .

# Install deps — numba pre-compiles on first run
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN mkdir -p data logs

EXPOSE 8080

CMD ["python", "-u", "bot.py"]
