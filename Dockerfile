FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=Asia/Kolkata

WORKDIR /app

RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin scalper \
    && mkdir -p /var/lib/scalper \
    && chown scalper:scalper /var/lib/scalper

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir .

USER scalper

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=3)"

CMD ["python", "-m", "scalper", "serve", "--host", "0.0.0.0", "--port", "8080"]
