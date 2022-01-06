install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

lint:
	pylint --disable=R,C,W,E1101,E1136,E1137 dags/premier_league_dag.py

format:
	black *.py

test:
	python -m pytest -vv --cov=dags/premier_league_dag.py tests/test_scrapper.py
