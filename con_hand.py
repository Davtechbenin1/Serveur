# coding:utf-8
"""
	Création de service de gestion de donnée via FastAPI (JSON + Binaires)
"""
import json, os, zipfile, shutil
import threading
import datetime
import concurrent.futures
from pathlib import Path

import psycopg2, sys
from psycopg2 import OperationalError, sql
from dotenv import load_dotenv
import os,asyncio
from psycopg2.pool import SimpleConnectionPool

load_dotenv()
DATABASE_URL= "postgresql://postgres:OjAXnBDSJNNzqnrCMgJbLvmQHFkhUwac@caboose.proxy.rlwy.net:23351/railway"

#os.getenv("DATABASE_URL")

def Arrondit_a(fonc):
	def wrapper(self,*args,**kwarg):
		num = fonc(self,*args,**kwarg)
		rest = str(int(num)).zfill(10)
		return args[0] + rest + f'.{args[-1]}'
	return wrapper

class Data_handler:
	def __init__(self):
		self.lock = threading.Lock()
		self.executor = concurrent.futures.ThreadPoolExecutor(
			max_workers=os.cpu_count() or 4
		)
		self.format_time = "%d-%m-%Y .%H:%M:%S.%f"
		self.file_base_dir = '.'

		self.created_table = set()

		self.Data_Table = {}
		self.Connexion_table = {}
		self.Arch_Data_Table = {}
		self.Update_Table = {}
		self.pool = SimpleConnectionPool(minconn=1, maxconn=3, dsn=DATABASE_URL)

		#self.conn = psycopg2.connect(DATABASE_URL)
		#self.conn.autocommit = True

	def get_conn(self):
		return self.pool.getconn()

	def put_conn(self,conn):
		self.pool.putconn(conn)

	def close_all(self):
		self.pool.closeall()

	def Create(self, base_name):
		base_name = base_name.lower()
		if base_name not in self.Data_Table:
			self.Data_Table[base_name] = {}
			self.Arch_Data_Table[base_name] = {}
			self.Update_Table[base_name] = {}
			self.connect_to(base_name)

	def connect_to(self, base_name):
		...

	
	def Save_binarie(self, file_name, file):
		content = file.file.read()
		self.executor.submit(self._Save_bin_, file_name, content)

	def _Save_bin_(self, file_name, content):
		file_path = os.path.join(self.file_base_dir, file_name)
		with open(file_path, "wb") as f:
			f.write(content)

	def update_data(self, base_name, table, data, ident):
		table = self.get_th_table(base_name, table)
		with self.lock:
			table_dict = self.Data_Table[base_name
				].get(table, dict())
			table_dict[data.get('id')] = data
			self.Data_Table[base_name][table] = table_dict
		conn = self.Create_table(base_name, table)
		cur = conn.cursor()

		query = sql.SQL("""
			UPDATE {table}
			SET data = %s::jsonb,
				updated_at = %s
			WHERE id = %s
		""").format(table=sql.Identifier(table))

		cur.execute(query, [json.dumps(data), 
			datetime.datetime.now(), ident])
		conn.commit()
		cur.close()
		return data

	def Get_binaire(self, file_name):
		file_path = os.path.join(self.file_base_dir, file_name)
		return file_path if os.path.exists(file_path) else None

	def get_th_table(self,base_name, table):
		return f"{base_name.lower()}__aa__{table.lower()}"

	@Arrondit_a
	def File_name(self, locl, extention):
		compteur_path = os.path.join(self.file_base_dir,locl)
		os.makedirs(compteur_path, exist_ok=True)
		fil =  "Compteur.json"
		compteur_path = os.path.join(compteur_path,fil)
		compteur_data = self.Get_fichier(compteur_path)
		current = compteur_data.get(extention, 0) + 1
		compteur_data[extention] = current
		self.Save_fichier(compteur_path, compteur_data)
		return current

	def real_ident(self, table, ident):
		if isinstance(ident, int):
			b_n = table[:3].upper()
			ident = str(ident)
			while len(ident) < 5:
				ident = '0' + ident
			return b_n + ident
		else:
			return ident

	def get_ident(self, ident):
		return ident

	def Create_table(self, base_name, table):
		if table in self.created_table:
			return self.get_conn()
		con = self.get_conn()
		cur = con.cursor()
		query = sql.SQL("""
			CREATE TABLE IF NOT EXISTS {table} (
				id TEXT PRIMARY KEY,
				data JSONB NOT NULL,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			)
		""").format(table=sql.Identifier(table))
		cur.execute(query)
		con.commit()
		cur.close()
		self.created_table.add(table)
		return con

	def clean_old_log(self):
		dossier = Path("Log")
		if dossier.exists():
			folder_list = []
			if len(list(dossier.iterdir())) > 3:
				for folder in dossier.iterdir():
					if folder.is_dir():
						folder_list.append(datetime.datetime.strptime(folder.name, "%Y-%m-%d"))
			if folder_list:
				folder_list.sort()
				sup_list = folder_list[:-3]
				for date in sup_list:
					name = date.strftime("%Y-%m-%d")
					sup_path = Path(os.path.join(dossier, name))
					if sup_path.exists():
						shutil.rmtree(sup_path)

	def Get_backup(self, base_name):
		"""
		Crée un fichier ZIP contenant les données de toutes les tables
		d'une base de données PostgreSQL.
		Chaque table est sauvegardée sous forme d'un fichier JSON.
		"""
		conn = self.get_conn()
		
		# Nom du fichier ZIP
		ZIP_p = os.path.join(getattr(self, "arch_base_dir", "."), f"{base_name}_backup.zip")

		with self.lock, zipfile.ZipFile(ZIP_p, "w", zipfile.ZIP_DEFLATED) as zipf:
			cur = conn.cursor()
			
			# Récupérer la liste des tables dans le schéma public
			cur.execute("""
				SELECT table_name
				FROM information_schema.tables
				WHERE table_schema = 'public'
			""")
			tables = [row[0] for row in cur.fetchall()]

			for table in tables:
				# Récupérer la ligne unique de chaque table
				cur.execute(sql.SQL("SELECT data FROM {} LIMIT 1").format(sql.Identifier(table)))
				row = cur.fetchone()
				if row:
					data = row[0]  # {id: dict}
				else:
					data = {}

				# Créer un fichier JSON temporaire en mémoire
				json_bytes = json.dumps(data, indent=4).encode("utf-8")
				zipf.writestr(f"{table}.json", json_bytes)

			cur.close()
		self.put_conn(conn)

		return ZIP_p

	def Get_fichier(self, fichier):
		if not fichier.endswith(".json"):
			fichier += ".json"
		try:
			with open(fichier, "r", encoding='utf-8') as f:
				return json.load(f)
		except Exception:
			return {}

	def Save_fichier(self, fichier, data):
		if not fichier.endswith(".json"):
			fichier += ".json"
		with self.lock:
			with open(fichier, "w", encoding='utf-8') as f:
				json.dump(data, f, indent=4)
			#os.replace(temp_file, fichier)

	def log(self, message):
		self._log(message)

	def _log(self, message):
		now = datetime.datetime.now()
		fic_name = now.strftime("%H-%M.txt")

		log_pat = Path("Log")
		os.makedirs(log_pat, exist_ok=True)
		log_pat = os.path.join(log_pat, now.strftime('%Y-%m-%d'))
		os.makedirs(log_pat, exist_ok=True)

		fic_name = os.path.join(log_pat, fic_name)
		with open(fic_name, 'a') as fic:
			fic.write(message)
			fic.write('\n\n')

	def drop_all_tables(self, dbname, user, password, host="localhost", port=5432):
		try:
			conn = self.get_conn()
			cur = conn.cursor()

			cur.execute("DROP SCHEMA public CASCADE;")
			cur.execute("CREATE SCHEMA public;")

			print("Toutes les tables ont été supprimées avec succès.")
			cur.close()
			self.put_conn(conn)

		except Exception as e:
			print("Erreur:", e)

# Gestion des entrées et sorties
	
	async def Get_data(self,base_name,table, ident=None):
		run_loop = asyncio.get_running_loop()
		return await run_loop.run_in_executor(None,
			self.__Get_data,base_name,
			table, ident)

	def __Get_data(self, base_name, table, ident=None):
		all_table = self.Data_Table.get(base_name, {})
		table = self.get_th_table(base_name, table)

		if table in all_table:
			table_cache = all_table.get(table)
			if ident:
				return table_cache.get(ident, {})
			return table_cache

		conn = self.Create_table(base_name, table)
		rows = dict()
		try:
			cur = conn.cursor()

			query = sql.SQL("SELECT id, data FROM {}"
				).format(sql.Identifier(table))
			cur.execute(query)
			rows = {row[0]: row[1] for row in cur.fetchall()}

			
			cur.close()
			

			# Mettre à jour le cache
			self.Data_Table.setdefault(base_name, {})[table] = rows
		
		finally:
			self.put_conn(conn)	
			if ident:
				return rows.get(ident, {})
			return rows


	async def Multiple_get(self,base_name,tables_liste):
		run_loop = asyncio.get_running_loop()
		return await run_loop.run_in_executor(None,
			self.__Multiple_get,
			base_name,tables_liste)

	def __Multiple_get(self,base_name,tables_liste):
		all_dic = dict()
		for table in tables_liste:
			all_dic[table] = self.__Get_data(base_name,table)
		return all_dic

	async def Save_data(self,base_name,table,data,data_ident):
		run_loop = asyncio.get_running_loop()
		return await run_loop.run_in_executor(None,
			self.__Save_data,
			base_name,table,data,data_ident)

	def __Save_data(self, base_name, table, data, data_ident):
		"""
		Sauvegarde ou met à jour une entrée dans la table.
		data_ident : identifiant de l'entrée (optionnel si 'id' présent dans data)
		"""

		table = self.get_th_table(base_name, table)
		conn = self.Create_table(base_name, table)
		try:
			cur = conn.cursor()

			# Récupérer la ligne existante
			query = sql.SQL("""
				INSERT INTO {table} (id, data)
				VALUES (%s, %s::jsonb)
				ON CONFLICT (id) DO UPDATE
				SET data = EXCLUDED.data, updated_at = NOW()
				""").format(table=sql.Identifier(table))
			cur.execute(query,[data_ident, json.dumps(data)])
			conn.commit()
			cur.close()
			self.Data_Table.setdefault(base_name, {}).setdefault(table, {})[data_ident] = data
		finally:
			self.put_conn(conn)

		return data

	async def Delete_data(self,base_name,table,ident):
		run_loop = asyncio.get_running_loop()
		return await run_loop.run_in_executor(None,self.__Delete_data,
			base_name,table,ident)

	def __Delete_data(self, base_name, table, ident):
		"""
		Supprime une ou plusieurs entrées dans la table.
		ident : int, str ou liste
		"""
		table = self.get_th_table(base_name, table)
		conn = self.Create_table(base_name, table)
		try:
			cur = conn.cursor()

			# Récupérer la ligne existante
			if isinstance(ident, list):
				for i in ident:
					cur.execute(
						sql.SQL("DELETE FROM {} WHERE id = %s").format(sql.Identifier(table)),
						[i]
					)
			else:
				cur.execute(
					sql.SQL("DELETE FROM {} WHERE id = %s").format(sql.Identifier(table)),
					[ident]
				)

			conn.commit()
			cur.close()
			table_cache = self.Data_Table.setdefault(base_name, {}).setdefault(table, {})
			if isinstance(ident, list):
				for i in ident:
					table_cache.pop(i, None)
			else:
				table_cache.pop(ident, None)

			return True
		finally:
			self.put_conn(conn)

		# mise à jour cache
		



