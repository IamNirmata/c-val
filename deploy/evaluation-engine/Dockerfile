FROM python:3.12-slim-bookworm
ARG CVAL_COMMIT
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 CVAL_EVALUATOR_SHA=${CVAL_COMMIT}
WORKDIR /app
COPY pyproject.toml README.md ./
COPY cval ./cval
COPY evaluation_engine ./evaluation_engine
COPY config ./config
COPY validation-tests ./validation-tests
COPY ymls ./ymls
RUN python -c "import os,re; value=os.environ['CVAL_EVALUATOR_SHA']; assert re.fullmatch('[0-9a-f]{40}',value) and value != '0'*40" \
    && pip install --no-cache-dir . \
    && python -m evaluation_engine --config config/cval.toml --check-config
ENTRYPOINT ["python", "-m", "evaluation_engine"]
CMD ["--config", "/app/config/cval.toml", "--confirm", "evaluate"]