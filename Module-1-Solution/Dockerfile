FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/
WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH"
COPY "pyproject.toml" "uv.lock" ".python-version" ./
RUN uv sync --locked
COPY module_1.py .
ENTRYPOINT ["python", "module_1.py"]
