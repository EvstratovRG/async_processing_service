FROM python:3.12-slim
WORKDIR /service
COPY --from=ghcr.io/astral-sh/uv:0.12.17 /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev
COPY app ./app
COPY alembic.ini ./
COPY migrations ./migrations
COPY scripts/docker ./scripts/docker
ENV PATH="/service/.venv/bin:$PATH" PYTHONUNBUFFERED=1
RUN useradd --create-home service
USER service
CMD ["/service/scripts/docker/api.sh"]
