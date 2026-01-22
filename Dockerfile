FROM python:3.10

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Expose port (Cloud Run uses PORT environment variable)
EXPOSE 8080

# Set environment variable for port
ENV PORT=8080

CMD ["python", "cdc_consumer.py"]