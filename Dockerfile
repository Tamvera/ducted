FROM python:3.12-trixie

WORKDIR /duct

RUN apt-get update
RUN apt-get -y upgrade

RUN mkdir -p /duct/conf.d

ADD duct duct
ADD pyproject.toml .
ADD docker/duct.yml.example duct.yml

RUN pip install -e .

USER 65534

CMD twistd --pidfile=/tmp/duct.pid -n duct -c /duct/duct.yml
