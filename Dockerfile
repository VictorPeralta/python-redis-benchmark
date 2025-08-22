FROM ghcr.io/astral-sh/uv:python3.12-trixie

COPY . /app

WORKDIR /app

CMD ["uv", "run", "pytest","--redis-host=redis"]
