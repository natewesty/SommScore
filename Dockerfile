# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV FLASK_APP=app.py
ENV FLASK_ENV=development
ENV FLASK_DEBUG=1
ENV DB_PATH=/data/commerce7.db

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create data directory first
RUN mkdir -p /data

# Copy project files
COPY . .

# Set permissions after copying files
RUN chown -R 1000:1000 /app /data && \
    chmod -R 755 /app && \
    chmod 777 /data

# Run as non-root user
USER 1000:1000

# Use gunicorn for production
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "app:app"] 