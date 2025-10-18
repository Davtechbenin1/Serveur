#Coding:utf-8
def Get_data(self, base_name, table, ident=None):
	all_table = self.Data_Table.get(base_name, {})
	table_cache = all_table.get(table, {})

	if table_cache:
		if ident:
			return table_cache.get(ident, {})
		else:
			return table_cache

	conn = self.Create_table(base_name, table)
	cur = conn.cursor()

	query = sql.SQL("SELECT data FROM {} LIMIT 1").format(sql.Identifier(table))
	cur.execute(query)
	row = cur.fetchone()

	if row:
		rows = row[0]  # {id: dict()}
	else:
		rows = {}

	cur.close()

	# Mettre à jour le cache
	self.Data_Table.setdefault(base_name, {})[table] = rows

	if ident:
		return rows.get(ident, {})
	return rows

def Save_data(self, base_name, table, data, data_ident):
	"""
	Sauvegarde ou met à jour une entrée dans la table.
	data_ident : identifiant de l'entrée (optionnel si 'id' présent dans data)
	"""
	th_ident = self.get_ident(data_ident)
	conn = self.Create_table(base_name, table)
	cur = conn.cursor()

	# Récupérer la ligne existante
	query = sql.SQL("SELECT data FROM {} LIMIT 1").format(sql.Identifier(table))
	cur.execute(query)
	row = cur.fetchone()
	if row:
		all_rows = row[0]  # {id: dict}
	else:
		all_rows = {}

	# Mettre à jour ou ajouter l'entrée
	all_rows[data_ident] = data

	# Mettre à jour la table
	if row:
		update_query = sql.SQL("UPDATE {table} SET data = %s::jsonb, updated_at = %s").format(
			table=sql.Identifier(table)
		)
		cur.execute(update_query, [json.dumps(all_rows), datetime.datetime.now()])
	else:
		insert_query = sql.SQL("INSERT INTO {table} (data, updated_at) VALUES (%s, %s)").format(
			table=sql.Identifier(table)
		)
		cur.execute(insert_query, [json.dumps(all_rows), datetime.datetime.now()])

	conn.commit()
	cur.close()

	# Mettre à jour le cache
	self.Data_Table.setdefault(base_name, {})[table] = all_rows

	return data

def Delete_data(self, base_name, table, ident):
	"""
	Supprime une ou plusieurs entrées dans la table.
	ident : int, str ou liste
	"""
	conn = self.Create_table(base_name, table)
	cur = conn.cursor()

	# Récupérer la ligne existante
	query = sql.SQL("SELECT data FROM {} LIMIT 1").format(sql.Identifier(table))
	cur.execute(query)
	row = cur.fetchone()
	if row:
		all_rows = row[0]  # {id: dict}
	else:
		all_rows = {}

	# Supprimer les entrées
	if isinstance(ident, list):
		for i in ident:
			all_rows.pop(i, None)
	else:
		all_rows.pop(ident, None)

	# Mettre à jour la table
	if row:
		update_query = sql.SQL("UPDATE {table} SET data = %s::jsonb, updated_at = %s").format(
			table=sql.Identifier(table)
		)
		cur.execute(update_query, [json.dumps(all_rows), datetime.datetime.now()])
		conn.commit()

	cur.close()

	# Mettre à jour le cache
	self.Data_Table.setdefault(base_name, {})[table] = all_rows

	return True
