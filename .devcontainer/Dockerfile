# Use a mabbaforge image with conda pre-installed
FROM quay.io/condaforge/mambaforge:24.9.2-0

# Install the conda lock into the base environment
RUN conda install -y \
    conda-lock \
    && conda clean -tipy

# Install dependencies from conda-lock file into a named project environment
COPY conda-linux-64.lock conda-linux-64.lock
RUN conda update --all --solver=classic -n base -c conda-forge conda && \
    conda create -y -n climate_futures --file conda-linux-64.lock && \
    conda clean -tipy

# Register the environment as a Jupyter kernel with a recognizable name
# (ipykernel is already provided by the lock file, so no extra conda solve is needed here)
RUN conda run -n climate_futures python -m ipykernel install \
        --name climate_futures --display-name "climate_futures"

# Install the repo using pip from the project environment
RUN conda run -n climate_futures pip install https://github.com/cal-adapt/climakitae/archive/refs/tags/1.4.0.zip

# Keep container alive
CMD ["tail", "-f", "/dev/null"]