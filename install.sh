#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
python3 -m virtualenv venv
echo "export PYTHONPATH=${PYTHONPATH}:${DIR}/src" >> ${DIR}/venv/bin/activate
source ${DIR}/venv/bin/activate
pip install -r ${DIR}/requirements.txt
