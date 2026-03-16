# Use a Conda base image which has Conda pre-installed
FROM continuumio/miniconda3

# Set the locale to prevent Unicode errors
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8

# Set the working directory
WORKDIR /app

# Copy the environment file
COPY requirements.yaml .

# Create the Conda environment from the file.
# This will create an environment named 'ma' as specified in the .yaml file.
RUN conda env create -f requirements.yaml

# Copy the rest of the application's code
COPY . .

# Find zipped files after cloning and unzips them in the same dir
RUN find results/msar/ -name "coefficients.csv.gz" -exec gunzip {} +

# Install the project in editable mode using the pip from the 'ma' environment.
# This makes the 'mef-tool' command available within the Conda environment.
RUN conda run -n ma pip install -e .

# --- Entrypoint Configuration ---
# This wrapper script ensures that any command runs within the activated 'ma' environment.
# This is especially useful for the interactive shell.
RUN echo '#!/bin/sh' > /entrypoint.sh && \
    echo 'eval "$(conda shell.bash hook)"' >> /entrypoint.sh && \
    echo 'conda activate ma' >> /entrypoint.sh && \
    echo 'exec "$@"' >> /entrypoint.sh && \
    chmod +x /entrypoint.sh

# Set the entrypoint to our wrapper script
ENTRYPOINT ["/entrypoint.sh"]

# Set the default command to show the help message for your tool
CMD ["mef", "--help"]
