FROM ghcr.io/emqx/emqx-builder/5.6-3:1.18.3-27.3.4.2-4-debian13 AS bench
ARG BENCH_REPO=https://github.com/Kit09K/emqtt-bench.git
ARG BENCH_REF=2b33877d4ca41b58fdf3732fbb13dca39b93988d
RUN git clone "$BENCH_REPO" /emqtt_bench && cd /emqtt_bench && git checkout "$BENCH_REF"
WORKDIR /emqtt_bench
ENV BUILD_WITHOUT_QUIC=1
RUN make release

FROM python:3.12-slim-trixie
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates procps && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir paho-mqtt==2.1.0 docker==7.1.0 mcp==2.3.0 reportlab==4.2.5
COPY --from=bench /emqtt_bench/emqtt-bench-*.tar.gz /emqtt_bench/
RUN cd /emqtt_bench && tar zxf emqtt-bench-*.tar.gz && rm emqtt-bench-*.tar.gz
WORKDIR /app
COPY app.py mcp_server.py report.py index.html test_app.py ./
EXPOSE 8080 8081 8082
# run as module "app" (not __main__) so mcp_server shares its state
CMD ["python", "-u", "-c", "import app; app.main()"]
