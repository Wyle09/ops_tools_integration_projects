FROM python:3.9-slim

RUN useradd --user-group --system --create-home --no-log-init embrace && mkdir /code

RUN apt-get update && apt-get -y install --no-install-recommends \
 gcc \
 libc-dev \
 libffi-dev \
 libmariadb-dev \
 libpcre3-dev \
 mariadb-client \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY . /code/

RUN pip install -r /code/requirements.txt && rm -rf ~/.cache/pip

WORKDIR /code

USER embrace

CMD ["/usr/bin/env", "python", "/code/ops_tools_data_loaders/ops_tools_data_loaders.py"]
