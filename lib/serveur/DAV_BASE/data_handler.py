#Coding:utf-8
"""
	Module de transformation des données à sauvegarder

	L'Objet à prendre en compte ici, c'est myData pour la
	séréalisation des données dans des fichiers avec les 
	même méthode simple comme pickler.
"""
class myData:
	def __init__(self):
		"""
			Fic peut être tout sorte d'objet accepttant les 
			méthodes write et read

			Le principe ici est une donnée par fichier.
		"""
		#self.Contener = fic
		self.Trans = _Transform()

	def dump(self,fic,data):
		# Data doit être une donnée parmie les types standart de python
		Data = self.Trans.convert(data)
		with open(fic,'wb') as fic:
			fic.write(Data.encode('utf8'))
		return len(Data)

	def load(self,fic):
		this_fic = fic
		with open(fic,'r',encoding = 'utf8') as fic:
			try:
				Read = fic.read()
			except UnicodeDecodeError:
				with open(this_fic,'r') as fic:
					R = fic.read()
				self.dump(this_fic,self.Trans.Restore(R))
				Read = R
		
		Read = self.Trans.Restore(Read)
		
		return Read

class _Transform:
	def __init__(self):
		"""
			Objet de transformation des données format text
			compréhensible pour la base
		"""
		self.simple_type = [int,float,str,bool]
		self.list_tuple_type = [list,tuple]

		self.type_dict = {
			"int":int,"str":str,"float":float,"list":list,
			"tuple":tuple,"dict":dict,'bool':bool
		}
		self.S_type = ["int","float",'str','bool']
		self.C_type = ["list",'tuple']

# Convert something to str
	def _type_simple(self,typ,val,sep):
		st = f"{typ}{sep}{val};"
		return st

	def _int_to_str(self,val,sep = ':'):
		st = self._type_simple("int",val,sep)
		return st

	def _float_to_str(self,val,sep = ':'):
		st = self._type_simple("float",val,sep)
		return st

	def _str_to_str(self,val,sep = ':'):
		st = self._type_simple("str",val,sep)
		return st

	def _bool_to_str(self,val,sep = ':'):
		st = self._type_simple("bool",int(val),sep)
		return st

	def _list_tuple(self,typ,val,sep):
		st = f"{typ}{sep}("
		for v in val:
			S = f"|{sep}|"
			if type(v) in self.simple_type or v == None:
				st_ = self.type_simple(v,S)

			elif type(v) in self.list_tuple_type:
				st_ = self.conteneur(v,S)

			elif type(v) == dict:
				st_ = self._dict_type(v,S)
			st += st_
			st += f' {sep};; '
		st += ')'
		return st

	def _list_to_str(self,val,sep = ":"):
		st = self._list_tuple('list',val,sep)
		return st

	def _tuple_to_str(self,val,sep = ":"):
		st = self._list_tuple('tuple',val,sep)
		return st

	def _dict_type(self,val,sep = ':'):
		st = 'dict'+sep+"["
		for key in val:
			this_value = str()
			S = f"{sep}->"
			if type(key) in self.simple_type or key == None:
				key_ = self.type_simple(key,S)

			elif type(key) in self.list_tuple_type:
				key_ = self.conteneur(key,S)

			elif type(key) == dict:
				key_ = self._dict_type(key,S)
			else:
				raise TypeError(f"Unknown Object type {type(key)}")
			this_value += key_
			this_value += f"|{sep}|"

			valeur = val[key]

			if type(valeur) in self.simple_type or valeur == None:
				valeur_ = self.type_simple(valeur,S)

			elif type(valeur) in self.list_tuple_type:
				valeur_ = self.conteneur(valeur,S)

			elif type(valeur) == dict:
				valeur_ = self._dict_type(valeur,S)
			else:
				#print()
				raise TypeError(f"Unknown Object type {type(valeur)}")

			this_value += valeur_
			this_value += f' {sep};;; '

			st += this_value
		st += ']'
		return st

	def type_simple(self,donner,sep = ":"):
		typ = type(donner)
		if typ == int:
			return self._int_to_str(donner,sep)
		elif typ == float:
			return self._float_to_str(donner,sep)
		elif typ == str:
			return self._str_to_str(donner,sep)
		elif typ == bool:
			return self._bool_to_str(donner,sep)
		elif donner == None:
			return self._type_simple('None',donner,sep)
		else:
			raise TypeError(f'Object type must be int, float or str not {typ}')

	def conteneur(self,donner,sep = ":"):
		typ = type(donner)
		if typ == list:
			return self._list_to_str(donner,sep)
		elif typ == tuple:
			return self._tuple_to_str(donner,sep)
		else:
			raise TypeError(f'Object type must be list or tuple not {typ}')

	def dic(self,donner):
		sep = ':'
		return self._dict_type(donner,sep)

	def convert(self,donner):
		if type(donner) in self.simple_type or donner == None:
			return self.type_simple(donner)
		elif type(donner) in self.list_tuple_type:
			return self.conteneur(donner)
		elif type(donner) == dict:
			return self.dic(donner)
		else:
			raise TypeError(f'Unknown Object type {type(donner)}')

# Convert str to sommeting
	def _str_simple_type(self,donner,sep = ':'):
		D = donner.split(';')[0]
		typ,val = D.split(sep)
		if typ == 'None':
			return None
		if typ == 'bool':
			return bool(int(val))
		val = self.type_dict[typ](val)
		return val

	def _str_conteneur_type(self,donner,sep = ':'):
		final_Typ,val= donner.split(f'{sep}(')
		lis = list()
		val = val[0:-1]
		val_list = val.split(f' {sep};; ')
		del val_list[-1]
		this_sep = f"|{sep}|"
		for his_val in val_list:
			this_sep = f"|{sep}|"
			typ = his_val.split(this_sep)[0]
			if typ in self.S_type:
				V = self._str_simple_type(his_val,this_sep)
			elif typ in self.C_type:
				V = self._str_conteneur_type(his_val,this_sep)
			elif typ == 'dict':
				V = self._str_dict_type(his_val,this_sep)
			elif typ == 'None':
				V = 0
			else:
				raise TypeError(f'Unknown contener type {typ}')
			lis.append(V)
		lis = self.type_dict[final_Typ](lis)
		return lis

	def _str_dict_type(self,donner,sep = ':'):
		#print(donner)
		#print()
		final_Typ,val = donner.split(f'{sep}[')
		dic = dict()
		val = val[0:-1]
		val_list = val.split(f' {sep};;; ')
		del val_list[-1]
		this_sep = f"|{sep}|"
		#print("------------")
		#print(val_list)
		for item in val_list:
			#print("****")
			#print(item)
			key,value = item.split(this_sep)
		# gestion_clé
			key_typ = key.split(f'{sep}->')[0]
			if key_typ in self.S_type or key_typ == 'None':
				dict_key = self._str_simple_type(key,f'{sep}->')
			elif key_typ in self.C_type:
				dict_key = self._str_conteneur_type(key,f'{sep}->')
			elif key_typ == 'dict':
				dict_key = self._str_dict_type(key,f'{sep}->')
			else:
				raise TypeError(f'Unknown contener type {key_typ}')
		# gestion_value
			value_typ = value.split(f'{sep}->')[0]
			if value_typ in self.S_type or value_typ == 'None':
				dict_value = self._str_simple_type(value,f'{sep}->')
			elif value_typ in self.C_type:
				dict_value = self._str_conteneur_type(value,f'{sep}->')
			elif value_typ == 'dict':
				dict_value = self._str_dict_type(value,f'{sep}->')
			else:
				raise TypeError(f'Unknown contener type {value_typ}')

			dic[dict_key] = dict_value

		return dic

	def Restore(self,donner):
		"""
			Il s'agit de la restauration d'un seul donnée. 
			pas du contenue du fichier
		"""
		sep = ':'
		if not donner:
			raise ValueError('Data is empty')
		typ = donner.split(sep)[0]
		if typ in self.S_type:
			donner = self._str_simple_type(donner,sep)
		elif typ in self.C_type:
			donner = self._str_conteneur_type(donner,sep)
		elif typ == 'dict':
			donner = self._str_dict_type(donner,sep)
		elif typ == 'None':
			donner = None
		else:
			raise TypeError(f'Unknown Object type {typ}')

		return donner
