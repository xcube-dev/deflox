FROM mambaorg/micromamba:1.4.2
LABEL authors="Thomas"
LABEL maintainer="xcube-team@brockmann-consult.de"
LABEL name=deflox-ingestion

# Set the working directory inside the container
WORKDIR /app

RUN mkdir "ingestion"

# Copy only the necessary files to the container
COPY --chown=mambauser:mambauser ingestion/* ./ingestion

# Install dependencies
COPY --chown=mambauser:mambauser ./environment.yml .
RUN micromamba install -y -n base -f environment.yml && micromamba clean -a -y

# Make the script executable
RUN chmod +x ingestion/ingest.py

# Specify the command to run the script
# This allows passing arguments from outside the container
ENTRYPOINT ["python", "ingestion/ingest.py"]
