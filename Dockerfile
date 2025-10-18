# Utiliser une image Python légère
FROM python:3.11-slim

# Définir le répertoire de travail
WORKDIR /app

# Copier les fichiers requirements.txt
COPY requirements.txt .

# Installer les dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Copier le reste du code
COPY . .

# Définir la variable d'environnement par défaut
ENV PORT=8000

# Script de démarrage pour uvicorn en lisant le port depuis l'environnement
CMD ["python", "-u", "-c", "import os; import uvicorn; port=int(os.environ.get('PORT', 8000)); uvicorn.run('main:app', host='0.0.0.0', port=port)"]
