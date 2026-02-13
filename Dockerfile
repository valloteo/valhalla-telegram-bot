FROM python:3.11-slim

# Install system libs needed by Pillow
RUN apt-get update && apt-get install -y \
    libjpeg62-turbo-dev \
    zlib1g-dev \
    libfreetype6-dev \
    libwebp-dev \
    libtiff5-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Porta usata da Flask/Render
ENV PORT=5000
EXPOSE 5000

# Avvio del bot
CMD ["python", "flask_app.py"]
