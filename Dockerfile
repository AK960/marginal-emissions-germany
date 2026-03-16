# Base Image
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

# Install some system tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    git \
    vim \
    curl \
    ca-certificates \
    bash-completion \
    && rm -rf /var/lib/apt/lists/*

# Install miniconda
RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh && \
    /bin/bash ~/miniconda.sh -b -p /opt/conda && \
    rm ~/miniconda.sh && \
    ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh && \
    echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc && \
    echo "conda activate mef-germany" >> ~/.bashrc

# Path to conda
ENV PATH="/opt/conda/bin:${PATH}"

# Deactivate conda base activation
RUN conda config --set auto_activate false

WORKDIR /app

# Create conda environment with dependencies
COPY requirements.yaml .

# Must accept terms
RUN conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main && \
    conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r

RUN conda env create -f requirements.yaml

# Copy code and unpack zipped files
COPY . .
RUN find results/msar/ -name "coefficients.csv.gz" -exec gunzip {} +

# Install cli tool
RUN conda run -n mef-germany pip install --root-user-action=ignore -e .

# Create entrypoint-script to load the environment
RUN echo '#!/bin/bash' > /entrypoint.sh && \
    echo 'eval "$(conda shell.bash hook)"' >> /entrypoint.sh && \
    echo 'conda activate mef-germany' >> /entrypoint.sh && \
    echo 'exec "$@"' >> /entrypoint.sh && \
    chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

# Start Bash by default
CMD ["/bin/bash"]