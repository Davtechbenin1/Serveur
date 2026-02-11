#Coding:utf-8
"""
	Gestion de la base Local sqlite
"""
import json, re, datetime, time
from psycopg2 import sql

from pathlib import Path
import json, sys
from datetime import datetime
import threading
import string
import traceback

def success_response(self,data,where,action):
	return {
		"status":"ok",
		"data":data,
		"message":f'{action} {where} went successfully',
		"action":action,
		"where":where
	}
def failed_response(self,data,where,action,E = None):
	if not E:
		E = traceback.format_exc()
	return {
		"status":"error",
		"data":data,
		"message":f'{action} {where} went wrong. \n this is what goes wrong:\n{E}',
		"action":action,
		"where":where
	}


def normalize_table_name(base_name: str, table: str) -> str:
	base = re.sub(r"[^a-zA-Z0-9_]", "_", base_name.strip())
	table = re.sub(r"[^a-zA-Z0-9_]", "_", table.strip())
	return f"{base}__{table}"

# =========================
# CREATE TABLE
# =========================
def create_table(self, conn, real_table):
	if real_table in self.created_tables:
		return 
	cur = conn.cursor()
	cur.execute(
		sql.SQL("""
			CREATE TABLE IF NOT EXISTS {table} (
				id SERIAL PRIMARY KEY,
				data JSONB NOT NULL,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			)
		""").format(table = sql.Identifier(real_table))
	)
	conn.commit()
	cur.close()
	self.created_tables.add(real_table)
	return 

# ==========================
# GET DATA
# ==========================
def _get_data(self, base_name, date):
	t = time.time()
	conn = self.get_conn()
	try:
		if date:
			real_table = normalize_table_name(base_name, date)
		else:
			real_table = base_name
		self.create_table(conn, real_table)
		cur = conn.cursor()
		
		cur.execute(
			sql.SQL("SELECT id, data, updated_at FROM {} ORDER BY id ASC").format(sql.Identifier(real_table))
		)
		rows = cur.fetchall()
		#print()
		#print(rows)
		cur.close()
		tabs_dic = {row[0]: row[1] for row in rows}
		return tabs_dic
	except:
		print(traceback.format_exc())
		conn.rollback()
		return dict()
	finally:
		self.put_conn(conn)

def get_data(self, base_name ,date: str):
	#try:
	#print(base_name,date)
	data = self._get_data(base_name,date)
	ret = self.success_response(data,date,'get')
	#print(ret)
	#except Exception as E:
	#	ret = self.failed_response("error",dict(),where,id,'get')
	return ret

# ==========================
# SAVE DATA
# ==========================
def save_data(self, base_name, date, data):
	if date:
		real_table = normalize_table_name(base_name, date)
	else:
		real_table = base_name

	#print(real_table,data)
	t = time.time()
	conn = self.get_conn()
	try:
		cur = conn.cursor()
		
		self.create_table(conn, real_table)
		cur.execute(
			sql.SQL("""
				INSERT INTO {} (data, updated_at)
				VALUES (%s, NOW())
			""").format(sql.Identifier(real_table)),
			(json.dumps(data),)
		)

		conn.commit()
		cur.close()
		#print("ok")
		return self.success_response(data,real_table,'save')
	except:
		conn.rollback()
		return self.failed_response(data,real_table,"save")
	finally:
		self.put_conn(conn)

# =============================
# TABLE LIST
# =============================
def get_all_tabs_of(self,base_name):
	prefix = base_name.lower()
	conn = self.get_conn()
	cur = conn.cursor()
	cur.execute("""
	    SELECT schemaname, tablename
	    FROM pg_tables
	    WHERE tablename ILIKE %s
	""", (prefix + '%',))

	tables = cur.fetchall()
	return tables

def get_all_msg_of(self,base_name):
	tables = self.get_all_tabs_of(base_name)
	all_data = {}
	#print(tables)
	for ident in tables:
		sche, tab = ident
		#print(tab)
		msg_dic = self._get_data(tab,str())
		all_data[tab] = msg_dic
	return all_data

# ==========================
# DELETE ALL
# ==========================
def drop_tables(self, tables: tuple):
    """
    tables = tuple de ('schema', 'table')
    ex: (('public','user_msg'), ('public','user_msg_log'))
    """
    if not tables:
        print("Aucune table à supprimer.")
        return

    conn = self.get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    for schema, table in tables:
        try:
            query = sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                sql.Identifier(schema),
                sql.Identifier(table)
            )
            cur.execute(query)
            print(f"🗑️ Table supprimée : {schema}.{table}")
        except Exception as e:

            print(f"{traceback.format_exc()}❌ Erreur suppression {schema}.{table} -> {e}")

    cur.close()
    conn.close()

def drop_all_tab_of(self,base_name):
	tables = self.get_all_tabs_of(base_name)
	self.drop_tables(tables)
	#tables = self.get_all_tabs_of(base_name)
	#print(tables)

# ------------------
def redo_ident(self,where):
	if where:
		p = "1234567890_"+string.ascii_lowercase
		th_txt = str()
		for i in where.lower():
			if i not in p:
				i = "xx"
			th_txt += i
		return th_txt
	return str()

def _up_cache_local(self,where,data,id = None):
	with self._lock_dict.setdefault(where,threading.Lock()):
		tab_dic = self._local_cache.get(where,dict())
		if id:
			tab_dic[id] = data
		else:
			tab_dic.update(data)
		self._local_cache[where] = tab_dic

