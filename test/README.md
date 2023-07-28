# create test environment

Note, the requirement.txt has been tested for python 3.10.6.

```bash
cd test
mkdir .testenv
python3 -m venv .testenv
source .testenv/bin/activate
pip install pip setuptools --upgrade
pip install wheel
pip install -r requirements.txt

# install flat-json package
pip install -e ..
```

# run tests
```bash
cd test
source .testenv/bin/activate
pytest
```