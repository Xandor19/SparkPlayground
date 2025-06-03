PROJECT_PATH=/home/xandor19/Projects/Spark
IN_PROJECT_PATH=SparkPlayground/utils

sudo -u postgres bash -c "psql -h 127.0.0.1 -p 5432 < $PROJECT_PATH/$IN_PROJECT_PATH/postgres-sakila-create-db.sql"

sudo -u postgres bash -c "psql -h 127.0.0.1 -p 5432 -d sakila < $PROJECT_PATH/$IN_PROJECT_PATH/postgres-sakila-schema.sql"

sudo -u postgres bash -c "psql -h 127.0.0.1 -p 5432 -d sakila < $PROJECT_PATH/$IN_PROJECT_PATH/postgres-sakila-insert-data.sql"