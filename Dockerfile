FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml ./
COPY multi_tenant_streaming ./multi_tenant_streaming
RUN pip install --no-cache-dir . \
    && useradd --create-home --uid 10001 streaming

USER streaming

ENTRYPOINT ["multi-tenant-streaming"]
CMD ["serve"]
