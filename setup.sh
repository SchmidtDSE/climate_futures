#!/bin/bash
# =============================================================================
# DSE Climate Data Environment Setup
# =============================================================================
#
# This script sets up everything you need to run the climate data pipeline.
# Run it once when you first clone the repo.
#
# Usage:
#   ./setup.sh          # Full setup (Python + R + Jupyter)
#   ./setup.sh python   # Python environment only
#   ./setup.sh r        # R environment only
#
# =============================================================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo ""
echo -e "${BLUE}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║          DSE Climate Data Environment Setup                   ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""

# -----------------------------------------------------------------------------
# Check prerequisites
# -----------------------------------------------------------------------------

echo -e "${YELLOW}Checking prerequisites...${NC}"

# Check conda
if ! command -v conda &> /dev/null; then
    echo -e "${RED}ERROR: conda not found${NC}"
    echo ""
    echo "Please install Miniconda first:"
    echo "  https://docs.conda.io/en/latest/miniconda.html"
    echo ""
    echo "For Apple Silicon Mac:"
    echo "  curl -O https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-arm64.sh"
    echo "  bash Miniconda3-latest-MacOSX-arm64.sh"
    exit 1
fi

echo -e "  ${GREEN}✓${NC} conda found: $(conda --version)"

# Check we're in the right directory
if [ ! -f "envs/py-env.yml" ]; then
    echo -e "${RED}ERROR: envs/py-env.yml not found${NC}"
    echo ""
    echo "Make sure you're running this from the DSE project root:"
    echo "  cd /path/to/DSE"
    echo "  ./setup.sh"
    exit 1
fi

echo -e "  ${GREEN}✓${NC} Running from DSE project root"
echo ""

# -----------------------------------------------------------------------------
# Parse arguments
# -----------------------------------------------------------------------------

INSTALL_PYTHON=true
INSTALL_R=true

if [ "$1" == "python" ]; then
    INSTALL_R=false
    echo -e "${YELLOW}Installing Python environment only${NC}"
elif [ "$1" == "r" ]; then
    INSTALL_PYTHON=false
    echo -e "${YELLOW}Installing R environment only${NC}"
else
    echo -e "${YELLOW}Installing full environment (Python + R + Jupyter)${NC}"
fi
echo ""

# -----------------------------------------------------------------------------
# Create Python environment
# -----------------------------------------------------------------------------

if [ "$INSTALL_PYTHON" = true ]; then
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}Creating Python environment (py-env)...${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo "This installs: climakitae, xarray, geopandas, dask, coiled, and more"
    echo "Expected time: 3-5 minutes"
    echo ""

    # Remove existing env if present
    if conda env list | grep -q "^py-env "; then
        echo "Removing existing py-env..."
        conda env remove -n py-env -y
    fi

    conda env create -f envs/py-env.yml

    echo ""
    echo -e "  ${GREEN}✓${NC} py-env created successfully"
    echo ""
fi

# -----------------------------------------------------------------------------
# Create R environment
# -----------------------------------------------------------------------------

if [ "$INSTALL_R" = true ]; then
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}Creating R environment (r-env)...${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo "This installs: R, tidyverse, ggplot2, sf, and more"
    echo "Expected time: 2-3 minutes"
    echo ""

    # Remove existing env if present
    if conda env list | grep -q "^r-env "; then
        echo "Removing existing r-env..."
        conda env remove -n r-env -y
    fi

    conda env create -f envs/r-env.yml

    echo ""
    echo -e "  ${GREEN}✓${NC} r-env created successfully"
    echo ""
fi

# -----------------------------------------------------------------------------
# Set up Jupyter kernel discovery
# -----------------------------------------------------------------------------

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Setting up Jupyter...${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Install nb_conda_kernels in base so Jupyter sees all envs
conda install -n base -c conda-forge nb_conda_kernels jupyterlab -y --quiet

echo ""
echo -e "  ${GREEN}✓${NC} Jupyter configured to discover all conda kernels"
echo ""

# -----------------------------------------------------------------------------
# Verify installation
# -----------------------------------------------------------------------------

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Verifying installation...${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

if [ "$INSTALL_PYTHON" = true ]; then
    echo "Testing py-env imports..."
    conda run -n py-env python -c "
import sys
sys.path.insert(0, 'lib')
from andrewAdaptLibrary import CatalogExplorer, VARIABLE_MAP
print('  ✓ andrewAdaptLibrary imports successfully')
print(f'  ✓ Variables available: {list(VARIABLE_MAP.keys())}')
"
fi

echo ""

# -----------------------------------------------------------------------------
# Done!
# -----------------------------------------------------------------------------

echo -e "${GREEN}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║                    Setup Complete!                            ║${NC}"
echo -e "${GREEN}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo ""
echo "  1. Start Jupyter:"
echo "     ${BLUE}jupyter lab${NC}"
echo ""
echo "  2. Open the tutorial notebook:"
echo "     ${BLUE}notebooks/python/Tutorial_Coiled_Setup.ipynb${NC}"
echo ""
echo "  3. Set up Coiled (for fast cloud processing):"
echo "     ${BLUE}coiled login${NC}"
echo ""
echo -e "${YELLOW}Quick reference:${NC}"
echo ""
echo "  Activate Python env:  ${BLUE}conda activate py-env${NC}"
echo "  Activate R env:       ${BLUE}conda activate r-env${NC}"
echo "  Start Jupyter:        ${BLUE}jupyter lab${NC}"
echo ""
