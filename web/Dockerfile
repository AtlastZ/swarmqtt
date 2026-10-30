FROM python:3.12-slim
RUN pip install --no-cache-dir paho-mqtt==2.1.0 docker==7.1.0 mcp==2.3.0
WORKDIR /app
COPY app.py mcp_server.py index.html test_app.py ./
EXPOSE 8080 8081
# run as module "app" (not __main__) so mcp_server shares its state
CMD ["python", "-u", "-c", "import app; app.main()"]
