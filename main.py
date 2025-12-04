#Coding:utf-8
"""
	Gestion des données du logiciel
"""
from fastapi import FastAPI, Request, UploadFile, File
from fastapi.middleware.gzip import GZipMiddleware
from fastapi import HTTPException
from fastapi.responses import FileResponse

import uvicorn
from con_hand import Data_handler
from ws_manager import ConnectionManager
import logging,sys,os

from fastapi import FastAPI
#from fastapi.routes import user_routes

import asyncio
import json
from fastapi import WebSocket, WebSocketDisconnect

Con_obj = Data_handler()
#Con_obj.drop_all_tables("ZoeCorpLiv".lower(),
# "postgres", "davtechbenin")
app = FastAPI()
app.add_middleware(GZipMiddleware, minimum_size = 500)
#app.include_router(user_routes.router)

# instance globale
ws_manager = ConnectionManager()


from fastapi.responses import FileResponse

# Version actuelle de ZoeMarket
CURRENT_VERSION = "1.0.1"
GITHUB_APK_URL = "https://github.com/Davtechbenin1/Progest/releases/download/V1.0.0/zoeshop.apk"
@app.get("/api/current_version")
def get_update_info():
    """
    Retourne les infos de mise à jour pour ZoeMarket.
    """
    return {
        "version": CURRENT_VERSION,
        "apk_url": GITHUB_APK_URL,
        "changelog": "Première version publique de ZoeMarket."
    }
	
@app.get("/api/open_table/{table_name}")
def Update_my_tabs(table_name):
	return Con_obj.Create(table_name)

@app.post('/api/backup/{basename}')
def Backup(basename):
	zip_f = Con_obj.Get_backup(basename)
	return FileResponse(zip_f,media_type = "application/zip",
		filename = os.path.basename(zip_f))

# Méthode de récupération de donnée
@app.put("/api/select/{basename}/{table}")
async def get_data(basename,table,request:Request):
	data = await request.json()
	keys = data.get('keys') or None
	ret = await Con_obj.Get_data(basename,table,keys)
	data['data'] = ret

	info = f"select <<{ret}>> from table:{basename}_{table} key: {keys}"
	Con_obj.log(info)

	return data


@app.put("/api/select/{basename}")
async def get_multiple_table(basename,request:Request):
	data = await request.json()
	keys = data.get('keys') or None
	table_liste = data.get('table liste')
	ret = await Con_obj.Multiple_get(basename,table_liste)
	data['data'] = ret

	info = f"select <<{ret}>> from table:{basename}_{table_liste}"
	Con_obj.log(info)
	return data

# Méthode de sauvegarde de donnée
@app.put("/api/insert/{basename}/{table}")
async def save_data(basename,table,request:Request):
	data = await request.json()
	keys = data.get('keys') or None
	th_data = data.get('data')
	ret = await Con_obj.Save_data(basename,table,th_data,keys)
	asyncio.create_task(
		ws_manager.broadcast_table_update(basename, {
			"action":"update",
			"type":"insert",
			'basename':basename,
			"table": table,
			"keys": keys,
			"data": th_data,
		})
	)
	
	info = f"insert <<{th_data}>> to table:{basename}_{table} key: {keys}"
	Con_obj.log(info)
	return data

# Méthode de Suppression de donnée
@app.put("/api/delete/{basename}/{table}")
async def delete_data(basename,table,request:Request):
	data = await request.json()
	keys = data.get('keys') or None
	ret = await Con_obj.Delete_data(basename,table)

	asyncio.create_task(
		ws_manager.broadcast_table_update(basename, {
			"action":"update",
			"type":"delete",
			'basename':basename,
			"table": table,
			"keys": [keys],
			"data": ret,
		})
	)
	
	info = f"delete <<{ret}>> from table:{basename}_{table} key: {keys}"
	Con_obj.log(info)
	
	data['data'] = ret
	return data

# Méthode de sauvegarde de fichier binaires
@app.post("/api/upload/{localisation}")
async def upload_file(localisation:str,file: UploadFile = File(...)):
	ext = os.path.splitext(file.filename)[1][1:]
	file_name = await Con_obj.File_name(localisation,ext)
	Con_obj.Save_binarie(file_name,file)
	return {"filename":file_name}

@app.get("/api/download/{file_name}")
async def download_file(file_name:str):
	file_path = Con_obj.Get_binaire(file_name)
	if file_path:
		return FileResponse(file_path, filename = file_name)
	else:
		raise HTTPException(status_code = 404, detail = "Fichier introuvable")

# Gestion du web socket
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
	"""
	Protocol simple côté WebSocket :
	messages JSON attendus: {"action":"subscribe","table":"xxx"}
						{"action":"get_updates_since","table":"xxx","last_sync":"%d-%m-%Y .%H:%M:%S.%f"}
						{"action":"insert","table":"xxx","keys":[...],"data": {...}}
						{"action":"delete","table":"xxx","keys":[...]}
						{"action":"ping"}
	Réponses envoyées en JSON texte.
	"""
	await ws_manager.connect(websocket)
	try:
		while True:
			raw = await websocket.receive_text()
			try:
				msg = json.loads(raw)
			except json.JSONDecodeError:
				await websocket.send_json({"error": "invalid_json"})
				continue

			action = msg.get("action")
			if action == "subscribe":
				table = msg.get("table")
				await ws_manager.subscribe(websocket, table)
				await websocket.send_json({"action":"subscribed", "table":table})

			elif action == "unsubscribe":
				table = msg.get("table")
				await ws_manager.unsubscribe(websocket, table)
				await websocket.send_json({"action":"unsubscribed", "table":table})

			else:
				await websocket.send_json({"error":"unknown_action"})
	except WebSocketDisconnect:
		await ws_manager.disconnect(websocket)

logging.basicConfig(
	format = "%(levelname)s: %(message)s",
	level = logging.INFO
)
'''
if __name__ == "__main__":
	inf_dic = Con_obj.Get_fichier('CON_INFO')
	#"""
	if inf_dic:
		host = inf_dic.get('host')
		port = inf_dic.get("port")
	else:
		host = "localhost"
		port = 8010
		inf_dic = {"host":host,'port':port,"user":"postgres",
			"pass_word":'davtechbenin',"postgres_host":"localhost",
			"postgres_port":"5432"}
		Con_obj.Save_fichier("CON_INFO",inf_dic)

	Con_obj.user = inf_dic.get('user').strip()
	Con_obj.pass_word = inf_dic.get('pass_word').strip()
	Con_obj.port = int(inf_dic.get('postgres_port'))
	Con_obj.host = inf_dic.get('postgres_host').strip()
	
	uvicorn.run(app,port = port,host = host,reload = False,
		log_level = "info")
	#"""
#'''





