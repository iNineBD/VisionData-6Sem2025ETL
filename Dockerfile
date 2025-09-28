FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/
COPY setup.py .

# Install the package
RUN pip install -e .

# Create logs directory
RUN mkdir -p /app/logs

# Set environment
ENV PYTHONPATH=/app

# Default command - run the main ETL process
CMD ["python", "-m", "src.main"]
