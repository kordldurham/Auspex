FROM python:3.12-slim
WORKDIR /app
COPY runner/requirements.txt /app/runner/requirements.txt
RUN pip install --no-cache-dir -r /app/runner/requirements.txt \
    && useradd --create-home --uid 10001 reader
COPY runner /app/runner
USER reader
ENTRYPOINT ["python", "-m", "runner"]
