#!/bin/bash

# Install Node.js via Conda
conda install -c conda-forge nodejs=20 -y

# Install Python dependencies
pip install -r requirements.txt

# Install JupyterLab extensions (prebuilt packages)
pip install jupyterlab-lsp jupyterlab-spellchecker jupyterlab-code-formatter

# Install language server for Python (required by JupyterLab-LSP)
pip install 'python-lsp-server[all]'

# Build JupyterLab with non-default settings to prevent memory issues
jupyter lab build --dev-build=False --minimize=False

