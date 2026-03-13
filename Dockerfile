# Use official Python lightweight image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH /app

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the API code and the working payloads
COPY api/ /app/api/
COPY working_payloads/ /app/working_payloads/

# Expose the port
EXPOSE 8000

# Run the application
WORKDIR /app/api
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
