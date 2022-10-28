python3 -m venv ./venv

source venv/bin/activate
pip install -r requirements.txt

cd application
python3 wsgi.py