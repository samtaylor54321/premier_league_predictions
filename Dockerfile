# Set base image (host OS)
FROM python:3.9.11-slim-buster

# Add non root user
RUN adduser --system --no-create-home nonroot

# Set the working directory in the container
WORKDIR /app

# Copy the content of the local src directory to the working directory
COPY requirements.txt .
COPY static/css /app/static/css
COPY templates /app/templates
COPY main.py .

# Install any dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Take non root role
USER nonroot

# By default, listen on port 5000
EXPOSE 8080/tcp

# Specify the command to run on container start
CMD [ "python", "./main.py" ]