FROM python:3.10.12-slim-bookworm as builder

RUN pip install hatch
COPY . /source
WORKDIR /source
RUN hatch build

FROM python:3.10.12-slim-bookworm

COPY --from=builder /source/dist/*.whl /tmp/

RUN pip install --no-cache-dir $(echo /tmp/*.whl)[browser]
