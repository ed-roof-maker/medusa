#!/usr/bin/bash
cwdf=$(realpath $0)
cwdd=$(dirname ${cwdf})
cd ${cwdd}
rm -fr venv
echo 'Virtual Env Removed'
