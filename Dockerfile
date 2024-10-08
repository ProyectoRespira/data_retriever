ARG PYTHON_VERSION=3.10-slim-buster

FROM python:${PYTHON_VERSION}

# Prevent Python from writing .pyc files and enable unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory
WORKDIR /app

RUN apt-get update && \
    apt-get install -y libgomp1 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*  # Clean up the package list to reduce image size

# Copy only requirements.txt to the container
COPY requirements.txt /app/
COPY run_app.sh /app/

# Install dependencies and clean up cache
RUN set -ex && \
    pip install --upgrade pip && \
    pip install -r /app/requirements.txt && \
    rm -rf /root/.cache/

# Copy the rest of the application files into the container
COPY . /app/

# Ensure run_app.sh is executable
RUN chmod +x /app/run_app.sh

# Expose the port for the app
EXPOSE 6789

# Define the default command
CMD ["/app/run_app.sh"]


# FROM python:${PYTHON_VERSION}

# ENV PYTHONDONTWRITEBYTECODE=1
# ENV PYTHONUNBUFFERED=1

# WORKDIR /app

# COPY requirements.txt /app/

# RUN set -ex && \
#     pip install --upgrade pip && \
#     pip install -r /app/requirements.txt && \
#     rm -rf /root/.cache/

# COPY . /app/

# EXPOSE 6789

# RUN chmod +x run_app.sh

# CMD ["/bin/sh", "-c", "/app/run_app.sh"]
# #CMD ["python3 -m mage start etl-pipeline/"]
# # CMD ["/bin/sh"]
