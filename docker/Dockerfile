FROM mambaorg/micromamba:1.4.7
LABEL authors="Thomas"
LABEL maintainer="xcube-team@brockmann-consult.de"
LABEL name=deflox-ingestion

ARG NEW_MAMBA_USER=deflox
ARG NEW_MAMBA_USER_ID=1000
ARG NEW_MAMBA_USER_GID=1000

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
USER root

# Update system and ensure that basic commands are available.
RUN apt-get -y update && \
    apt-get -y upgrade vim jq curl wget && \
    apt-get -y install git && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Magic taken from https://hub.docker.com/r/mambaorg/micromamba,
# section "Changing the user id or name"
RUN usermod "--login=${NEW_MAMBA_USER}" "--home=/home/${NEW_MAMBA_USER}" \
        --move-home "-u ${NEW_MAMBA_USER_ID}" "${MAMBA_USER}" && \
    groupmod "--new-name=${NEW_MAMBA_USER}" \
             "-g ${NEW_MAMBA_USER_GID}" "${MAMBA_USER}" && \
    # Update the expected value of MAMBA_USER for the
    # _entrypoint.sh consistency check.
    echo "${NEW_MAMBA_USER}" > "/etc/arg_mamba_user" && \
    :

ENV MAMBA_USER=$NEW_MAMBA_USER
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

USER $MAMBA_USER

COPY --chown=$MAMBA_USER:$MAMBA_USER ../environment.yml /tmp/environment.yml
RUN micromamba install --yes -n base --file /tmp/environment.yml && \
    micromamba clean --all --yes
ARG MAMBA_DOCKERFILE_ACTIVATE=1

RUN micromamba install -n base -c conda-forge pip

ADD docker /tmp/
WORKDIR /tmp

# Copy files for deflox source install
#COPY --chown=$MAMBA_USER:$MAMBA_USER ../setup.py /tmp/setup.py
COPY --chown=$MAMBA_USER:$MAMBA_USER ../deflox /tmp/deflox
COPY --chown=$MAMBA_USER:$MAMBA_USER pyproject.toml /tmp/

# Switch into /tmp to install deflox
WORKDIR /tmp

# Specify the command to run the script
# This allows passing arguments from outside the container
ENTRYPOINT ["/opt/conda/bin/python", "deflox/ingestion/ingest.py"]
