help:
	@echo "make create_db"
	@echo "	   run docker container with project db structure"
	@echo "make delete_db"
	@echo "	   stop and remove container with db"
	@echo "make populate"
	@echo "	   run pylint and mypy"
	@echo "make run"
	@echo "	   run project"
	@echo "make doc"
	@echo "	   build sphinx documentation"
create_db:
	docker-compose up --build -d
delete_db:
	docker-compose rm -fs
populate:
	python scripts/fill_db.py
calc:
	python scripts/calculate.py
