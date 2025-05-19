#Coding:utf-8
"""
	Getion des méthodes pour la sauvegarde 
	des informations au format json
"""
import json
from pathlib import Path

def get_path(fichier):
	this_path = Path.home() /".Commercio"
	if this_path.exists():
		pass
	else:
		this_path.mkdir()
	this_path = this_path / set_fic(fichier)
	#print(this_path)
	return this_path

def set_fic(fichier):
	if '.dav' not in fichier:
		fichier = fichier.strip()
		fichier += '.dav'
	return fichier


def Save(fichier,data):
	fichier = get_path(fichier)
	with open(fichier,"w",encoding = 'utf-8') as f:
		json.dump(data,f,ensure_ascii = False,indent=4)

def Get(fichier):
	fichier = get_path(fichier)
	try:
		with open(fichier,'r',encoding = "utf-8") as f:
			data_json = json.load(f)
		return data_json
	except FileNotFoundError:
		return dict()

def Update(fichier,key,data):
	all_data = Get(fichier)
	all_data[key] = data
	Save(fichier,all_data)

def Get_this(fichier,key):
	return Get(fichier).get(key)


