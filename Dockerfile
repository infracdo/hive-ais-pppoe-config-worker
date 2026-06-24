FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY main.py .
COPY genieacs.py .

# Set environment variables (can be overridden)
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["python", "main.py"]
