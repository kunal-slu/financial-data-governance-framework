FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt requirements-core.txt ./
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "examples/run_lightweight_demo.py"]
