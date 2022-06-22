# Set base image (host OS)
FROM python:3.9-slim-buster

# By default, listen on port 5000
EXPOSE 5000/tcp

# Set the working directory in the container
WORKDIR /app

# Copy the dependencies file to the working directory
COPY requirements.txt .
ADD static/css /app/static/css
ADD templates /app/templates

# Install any dependencies
RUN pip install -r requirements.txt

# Copy the content of the local src directory to the working directory
COPY web_app.py .

# Specify the command to run on container start
CMD [ "python", "./web_app.py" ]