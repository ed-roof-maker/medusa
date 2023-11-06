flake8 --max-line-length 100 --indent-size 3 --ignore E712,W504,E902 src/lib/medusa.py
if [ $? -ne 0 ]; then exit 1; fi
# Run tests before reinstalling
python3 src/lib/medusa.py
if [ $? -ne 0 ]; then echo 'Tests failed.'; exit 1; fi
pip3 uninstall medusa
cp ../../rlab_common/work/wheel/* wheel
pip3 wheel --wheel-dir=wheel --no-index --find-links=wheel $(realpath src)
pip3 install --user --no-index --find-links=wheel medusa
rm -fr /tmp/medusa

