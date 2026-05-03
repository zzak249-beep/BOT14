FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source files from root
COPY bot.py bingx.py strategy.py telegram.py ./

CMD ["python", "bot.py"]
