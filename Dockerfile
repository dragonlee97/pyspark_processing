FROM apache/spark-py:v3.4.0

USER root
# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV SPARK_HOME=/opt/spark

WORKDIR /app
COPY . .

# Install additional Python packages if needed
RUN pip install --no-cache-dir --upgrade -r requirements.txt

