FROM python:3.6-alpine
ENV BUILD_REQUIREMENTS "git gcc musl-dev libxml2-dev libxslt-dev libffi-dev g++ gfortran"
ENV REQUIREMENTS "bash make libxml2 libxslt postgresql-dev postgresql-client linux-headers libstdc++ lapack-dev"
ENV SCRAPER_PAGES_LIMIT "0"

RUN mkdir -p /srv
WORKDIR /srv

COPY Pipfile .
COPY Pipfile.lock .

RUN apk update --no-cache \
    && apk add --no-cache $BUILD_REQUIREMENTS $REQUIREMENTS \
    && pip3 install pipenv==2018.11.26  \
    && pipenv install --system --deploy \
    && apk del $BUILD_REQUIREMENTS

COPY . .

EXPOSE 3000
ENTRYPOINT ["gunicorn"]
CMD ["--bind", "0.0.0.0:3000", "-k", "gevent", "--workers", "2", "application.wsgi:app"]

