#!/usr/bin/env bash
venv_name=venv-sm-demo

if [[ ! -f ${venv_name}/bin/activate ]]; then
    python3 -m venv ${venv_name}
fi

source ${venv_name}/bin/activate
pip install --upgrade pip setuptools wheel # Since venv installs older version of these.
pip install --upgrade -r requirements.txt

# Make virtual environment available as an ipython kernel
ipython kernel install --user --name=${venv_name}
