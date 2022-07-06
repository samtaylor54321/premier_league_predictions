install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

lint:
	pylint web_app.py

format:
	black *.py

test:
	python -m pytest -vv --cov=web_app.py tests/test_scrapper.py
