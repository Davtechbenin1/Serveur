#Coding:utf-8

from fastapi import FastAPI, HTTPException, UploadFile, File
from pydantic import BaseModel
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import uuid, os, shutil, pathlib

from typing import Optional
try:
	from base_hand import *
except ImportError:
	from .base_hand import *

import json

app = FastAPI()
CURENT_VERSION = "1.0.1"
UPLOAD_DIR = pathlib.Path('images')
UPLOAD_DIR.mkdir(exist_ok = True)

app.mount('/images',StaticFiles(directory = str(UPLOAD_DIR)),
	name = "images")

@app.post("/save-image")
async def updload_image(file:UploadFile = File(...)):
	if file.content_type.split('/')[0] != "image":
		raise HTTPException(status_code = 400, detail = "Le fichier doit être une image.")
	ext = pathlib.Path(file.filename).suffix
	filename = f"{uuid.uuid4().hex}{ext}"
	dest = UPLOAD_DIR/filename
	with dest.open("wb") as buffer:
		shutil.copyfileobj(file.file, buffer)
	url = f"/images/{filename}"
	return JSONResponse({"filename":filename, "url":url})

class data_form(BaseModel):
	data:dict

class date_data(BaseModel):
	date:str
	fichier:str
	data:dict

general_data = Get('Général')

# Gestion des données de la base général
@app.get("/Général/{fichier}")
async def read_general(fichier: str):
	return general_data.get(fichier,dict())


@app.get("/Général")
async def read_all_general():
	return general_data

@app.get("/")
async def home():
	return JSONResponse({"bonjour":"Bienvenue chez nous"})

@app.put('/Général/{fichier}')
async def put_general(fichier : str, data: dict):
	general_data[fichier] = data
	Update("Général",fichier,data)
	return {"Message":'Donnée ajouter'}

# Gestion des données en détails
"""
	Ici, on suppose que le fichier est combiné
	avec la date
"""
@app.get("/Détails/{fichier}")
async def read_details(fichier: str):
	return Get(fichier)


@app.put("/Détails/{fichier}")
async def put_details(fichier : str, data: dict):
	Save(fichier,data)
	return {"Méssage":"Donnée ajouter"}

@app.get("/")
def read_root():
	return {"message": "Serveur opérationnel"}


@app.get('/version')
async def get_version():
	return {"version":CURENT_VERSION}

@app.get('/update')
async def download_update():
	return FileResponse('./updates/v1.0.1.zip',filename = "update.zip")
