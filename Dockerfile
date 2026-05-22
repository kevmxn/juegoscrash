# ─── BUILD ────────────────────────────────────────────────────────────────────
FROM python:3.11-slim

# Evita archivos .pyc y activa logs sin buffer (esencial para ver logs en Render)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Instalar dependencias primero (capa cacheada si requirements no cambia)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY main.py .

# Puerto que Render asigna via variable PORT
EXPOSE 8080

# Arranque
CMD ["python", "main.py"]
