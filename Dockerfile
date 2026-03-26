FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:${PATH}"

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY main.py ./main.py
COPY README.md ./README.md

CMD ["python", "main.py"]
