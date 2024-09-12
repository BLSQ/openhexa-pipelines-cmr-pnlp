FROM blsq/openhexa-blsq-environment:latest

LABEL org.opencontainers.image.source=https://github.com/blsq/openhexa-pipelines-cmr-pnlp/docker
LABEL org.opencontainers.image.description="Base OpenHEXA image with additional tools for MS Access"

USER root

RUN apt-get update && \
    apt-get install -y mdbtools && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

USER ${NB_UID}
WORKDIR $HOME