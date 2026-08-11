# Use a mabbaforge image with conda pre-installed
FROM quay.io/condaforge/mambaforge:24.9.2-0

ARG TARGETARCH

# Install the conda lock into the base environment
RUN conda install -y \
    conda-lock \
    && conda clean -tipy

# Install dependencies into a named project environment.
# amd64 has a pinned lock file for reproducible builds; arm64 doesn't have one
# yet, so it solves fresh from environment.yml.
COPY conda-linux-64.lock conda-linux-64.lock
COPY environment.yml environment.yml
RUN conda update --all --solver=classic -n base -c conda-forge conda && \
    if [ "$TARGETARCH" = "amd64" ]; then \
        conda create -y -n climate_futures --file conda-linux-64.lock; \
    else \
        conda env create -n climate_futures --solver=classic -f environment.yml; \
    fi && \
    conda clean -tipy

# Register the environment as a Jupyter kernel with a recognizable name
# (ipykernel is already provided by the lock file, so no extra conda solve is needed here)
RUN conda run -n climate_futures python -m ipykernel install \
        --name climate_futures --display-name "climate_futures"

# Keep container alive
CMD ["tail", "-f", "/dev/null"]