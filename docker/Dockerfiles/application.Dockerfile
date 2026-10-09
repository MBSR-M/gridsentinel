FROM python:3.12.12-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --no-deps -r requirements.txt \
    && groupadd --gid 10001 gridsentinel \
    && useradd --uid 10001 --gid 10001 --no-create-home gridsentinel
COPY --chown=10001:10001 src /app/src
COPY --chown=10001:10001 config /app/config
COPY --chown=10001:10001 database /app/database
COPY --chown=10001:10001 scripts /app/scripts
COPY --chown=10001:10001 web /app/web
RUN mkdir -p /app/.local && chown 10001:10001 /app/.local
USER 10001:10001
EXPOSE 8000
STOPSIGNAL SIGTERM
CMD ["python", "-m", "gridsentinel", "api"]
