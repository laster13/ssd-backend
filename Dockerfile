FROM python:3.12-slim AS builder

ENV POETRY_VIRTUALENVS_CREATE=false

WORKDIR /app

RUN pip install --no-cache-dir poetry poetry-plugin-export

COPY pyproject.toml poetry.lock* ./

RUN poetry export -f requirements.txt --without-hashes -o /tmp/requirements.txt


FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
ENV FORCE_COLOR=1
ENV PYTHONPATH=/app/src
ENV ANSIBLE_LOCALHOST_WARNING=False
ENV ANSIBLE_INVENTORY_UNPARSED_WARNING=False
ENV ANSIBLE_PYTHON_INTERPRETER=/usr/local/bin/python3

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    curl \
    jq \
    sudo \
    sqlite3 \
    gettext-base \
    docker-cli \
    ca-certificates \
    apache2-utils \
    procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sfn /usr/local/bin/python3 /usr/bin/python3

WORKDIR /app

COPY --from=builder /tmp/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && pip install --no-cache-dir \
        ansible-core \
        docker \
        requests \
        jmespath \
        passlib \
    && ansible-galaxy collection install community.docker \
    && rm -rf \
        /root/.cache \
        /tmp/requirements.txt \
        /tmp/* \
        /var/tmp/* \
        /usr/local/share/doc \
        /usr/local/share/man \
        /usr/local/share/jupyter \
    && find /usr/local -type d -name "__pycache__" -prune -exec rm -rf {} + \
    && find /usr/local -type d -name "tests" -prune -exec rm -rf {} + \
    && find /usr/local -type f -name "*.pyc" -delete \
    && find /usr/local -type f -name "*.pyo" -delete \
    && find /root/.ansible -type d -name "tests" -prune -exec rm -rf {} + \
    && find /root/.ansible -type d -name "docs" -prune -exec rm -rf {} +

COPY . .

RUN mkdir -p /app/data /app/logs \
    && ln -sfn /app /ssd-backend

EXPOSE 8080

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080"]