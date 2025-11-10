# Use official Python image as base
FROM python:3.12-slim AS base


# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file and install Python dependencies
COPY requirements.txt .
RUN python -m pip install --upgrade pip setuptools wheel && \
    python -m pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application code
COPY . .

# ========= Development stage =========
FROM base AS development

# Install storm_watch package in editable mode
RUN pip install --no-cache-dir -e .

# Set environment variables (optional)
ENV PYTHONUNBUFFERED=1

# ========= Production stage =========
FROM base AS production

# Install storm_watch package in editable mode
RUN pip install --no-cache-dir .

# Set environment variables (optional)
ENV PYTHONUNBUFFERED=1

# clean the source in /app to avoid security issue
RUN rm -rf /app
