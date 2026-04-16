# Optional: local preview of the static site + Python tooling
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8000
CMD ["python", "-m", "http.server", "8000", "--directory", "web"]
