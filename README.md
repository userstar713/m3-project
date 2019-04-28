Populate domain_attribute table using tools/populate_domain_attributes.py

add source to the "source" database table before running syncronization


GET /api/1/source/:source_id/sync/


## Start service with docker containers
1. Dump/restore remote prod database:
    a) `pg_dump -Fd -f m3_prod -h terraform-00d41ec4239d29679706e2f698.cm1wf8rz8bru.us-east-1.rds.amazonaws.com -U dbadmin -d legoly_v2 -j 6 -W`
    b) `pg_restore -d m3 m3_prod -c -U dbadmin`
    You can also use the actual prod DB, you should set `SQLALCHEMY_DATABASE_URI` env variable to point to the prod db
2. Build & start dokcer image: `docker-compose up -d & docker-compose logs -f`
3. Start sync: `curl -X GET localhost:8000/api/1/source/1/sync/`

## How to start scraper locally:
1. Install pipenv: `pipenv install`
2. Activate virtualenv: `pipenv shell`
3. Start scraper manually:
    `python application/spiders/klwines.py`
    
# Start service for routes
cd application
flask run -p 5000
