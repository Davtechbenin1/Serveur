# Coding:utf-8
import json, re, datetime, time
from psycopg2 import sql

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
			CREATE TABLE IF NOT EXISTS {} (
				id TEXT PRIMARY KEY,
				data JSONB NOT NULL,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			)
		""").format(sql.Identifier(real_table))
	)
	conn.commit()
	cur.close()
	self.created_tables.add(real_table)
	return 

# ==========================
# GET DATA
# ==========================
def get_data(self, base_name, table, ident=None):
	t = time.time()
	conn = self.get_conn()
	try:
		real_table = normalize_table_name(base_name, table)
		self.create_table(conn, real_table)
		cur = conn.cursor()

		all_table_info = self.Data_Table.setdefault(real_table,dict())

		if ident:
			cur.execute(
				sql.SQL("SELECT data FROM {} WHERE id = %s").format(sql.Identifier(real_table)),
				(ident,)
			)
			row = cur.fetchone()
			th_row = row[0] if row else None
			cur.close()
			all_table_info[ident] = th_row
			self.Data_Table[real_table] = all_table_info
			
			return th_row
		else:
			cur.execute(
				sql.SQL("SELECT id, data FROM {}").format(sql.Identifier(real_table))
			)
			rows = cur.fetchall()
			cur.close()
			tabs_dic = {row[0]: row[1] for row in rows}
			all_table_info.update(tabs_dic)
			self.Data_Table[real_table] = all_table_info
			return tabs_dic
	finally:
		self.put_conn(conn)

# ==========================
# SAVE DATA
# ==========================
def save_data(self, base_name, table, data, ident):
	"""
	ident : ident unique côté client
	data : dict
	"""
	t = time.time()
	if not ident:
		raise ValueError("ident is required")
	conn = self.get_conn()
	try:
		real_table = normalize_table_name(base_name, table)
		
		cur = conn.cursor()
		
		self.create_table(conn, real_table)

		cur.execute(
			sql.SQL("""
				INSERT INTO {} (id, data, updated_at)
				VALUES (%s, %s, NOW())
				ON CONFLICT (id) DO UPDATE
				SET data = EXCLUDED.data,
					updated_at = NOW()
			""").format(sql.Identifier(real_table)),
			(ident, json.dumps(data))
		)

		conn.commit()
		cur.close()
		return data
	finally:
		self.put_conn(conn)

# ==========================
# DELETE DATA
# ==========================
def delete_data(self, base_name, table, ident):
	"""
	ident : str | list[str]
	"""
	if not ident:
		return False
	conn = self.get_conn()
	try:
		real_table = normalize_table_name(base_name, table)
		self.create_table(conn, real_table)
		cur = conn.cursor()

		if isinstance(ident, list):
			cur.execute(
				sql.SQL("DELETE FROM {} WHERE id = ANY(%s)").format(sql.Identifier(real_table)),
				(ident,)
			)
		else:
			cur.execute(
				sql.SQL("DELETE FROM {} WHERE id = %s").format(sql.Identifier(real_table)),
				(ident,)
			)
		
		conn.commit()
		cur.close()
		return True
	finally:
		self.put_conn(conn)
