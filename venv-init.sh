#!/usr/bin/bash
cwdf=$(realpath $0)
cwdd=$(dirname ${cwdf})
cd ${cwdd}
mkdir venv
virtualenv -p python3 venv
echo 'Virtual Env Created'
