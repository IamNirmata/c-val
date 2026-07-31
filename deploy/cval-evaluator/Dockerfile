ARG BUILDER_IMAGE=python:3.11-slim-bookworm@sha256:0000000000000000000000000000000000000000000000000000000000000000
ARG RUNTIME_IMAGE=gcr.io/distroless/python3-debian12:nonroot@sha256:0000000000000000000000000000000000000000000000000000000000000000

FROM ${BUILDER_IMAGE} AS install
ARG BUILD_COMMIT
ARG PYYAML_WHEEL_SHA256
WORKDIR /build

COPY deploy/cval-evaluator/requirements-evaluator.lock /build/requirements-evaluator.lock
COPY deploy/cval-evaluator/wheelhouse/ /wheelhouse/
COPY dist/cval-0.2.0-py3-none-any.whl /wheelhouse/cval-0.2.0-py3-none-any.whl

RUN test "${#BUILD_COMMIT}" -eq 40 \
    && case "${BUILD_COMMIT}" in *[!0-9a-f]*|0000000000000000000000000000000000000000) exit 1;; esac \
    && test "${#PYYAML_WHEEL_SHA256}" -eq 64 \
    && case "${PYYAML_WHEEL_SHA256}" in *[!0-9a-f]*|0000000000000000000000000000000000000000000000000000000000000000) exit 1;; esac \
    && PYYAML_WHEEL="$(find /wheelhouse -maxdepth 1 -type f -name 'PyYAML-6.0.2-*.whl')" \
    && test -n "${PYYAML_WHEEL}" \
    && test "$(printf '%s\n' "${PYYAML_WHEEL}" | wc -l)" -eq 1 \
    && printf '%s  %s\n' "${PYYAML_WHEEL_SHA256}" "${PYYAML_WHEEL}" | sha256sum -c - \
    && python -m pip install \
        --disable-pip-version-check \
        --no-cache-dir \
        --no-compile \
        --no-index \
        --find-links=/wheelhouse \
        --target=/workspace/c-val \
        -r /build/requirements-evaluator.lock \
    && python -m pip install \
        --disable-pip-version-check \
        --no-cache-dir \
        --no-compile \
        --no-deps \
        --no-index \
        --target=/workspace/c-val \
        /wheelhouse/cval-0.2.0-py3-none-any.whl \
    && printf '%s\n' "${BUILD_COMMIT}" > /workspace/c-val/cval/evaluator/BUILD_COMMIT

COPY config/cval.toml /app/config/cval.toml
COPY validation-tests/storage/test_config.toml /workspace/c-val/validation-tests/storage/test_config.toml
COPY validation-tests/storage/plugin.py /workspace/c-val/validation-tests/storage/plugin.py
COPY validation-tests/nccl/test_config.toml /workspace/c-val/validation-tests/nccl/test_config.toml
COPY validation-tests/nccl/plugin.py /workspace/c-val/validation-tests/nccl/plugin.py
COPY validation-tests/dltest/test_config.toml /workspace/c-val/validation-tests/dltest/test_config.toml
COPY validation-tests/dltest/plugin.py /workspace/c-val/validation-tests/dltest/plugin.py

RUN find /workspace/c-val /app -type d -exec chmod 0555 {} + \
    && find /workspace/c-val /app -type f -exec chmod 0444 {} +

FROM ${RUNTIME_IMAGE} AS evaluator
ARG BUILD_COMMIT
LABEL org.opencontainers.image.title="cval-evaluator" \
      org.opencontainers.image.revision="${BUILD_COMMIT}"

COPY --from=install --chown=65532:65532 /workspace/c-val /workspace/c-val
COPY --from=install --chown=65532:65532 /app/config /app/config

ENV HOME=/tmp \
    TMPDIR=/tmp \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/workspace/c-val \
    PYTHONUNBUFFERED=1
WORKDIR /workspace/c-val
USER 65532:65532
ENTRYPOINT ["/usr/bin/python3", "-m", "cval.cli"]
