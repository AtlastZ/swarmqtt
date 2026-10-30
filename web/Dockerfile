FROM python:3.12-slim
RUN pip install --no-cache-dir paho-mqtt==2.1.0 docker==7.1.0
WORKDIR /app
COPY app.py index.html test_app.py ./
EXPOSE 8080
CMD ["python", "-u", "app.py"]
