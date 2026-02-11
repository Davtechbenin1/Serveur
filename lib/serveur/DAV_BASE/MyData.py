#Coding:utf-8
try:
	from . import info
	from .data_handler import myData
except ImportError:
	import info
	from data_handler import myData
desc = """
	cet projet consiste a concevoir une base de donnée complet en python.
	Nous avons trois partie distinct:
		- Create
		- Update
		- Use
	D'abord, nous avons un seul fichier de base qui contient tous les bases
	qui seront créer. Le contenu est un dictionnaire dans lequel est stocké
	les objets identifiables par la clé.
"""
from pathlib import Path

import sys,os
from datetime import datetime
def open_fic(nom,path):
	while nom[-1] == ' ':
		nom = nom[:-1]
	name = f"{path}/{nom}"
	donn = myData().load(name)
	return donn

def _save_fic(nom,path,data = dict()):
	while nom[-1] == ' ':
		nom = nom[:-1]
	name = f"{path}/{nom}"
	myData().dump(name,data)

def save_fic(nom,path,data = dict()):
	_save_fic(nom,path,data)

class date_obj:
	def __init__(self,date = None):
		self.GENE_INF0 = now = datetime.now()
		if date:
			if '/' in date:
				date = date.split('/')
			elif '-' in date:
				date = date.split('-')
			self.day = int(date[0])
			self.month = int(date[1])
			self.year = int(date[2])
		else:
			
			self.day = now.day
			self.month = now.month
			self.year = now.year

		self.hur = now.hour
		self.minute = now.minute
		self.sec = now.second

		#DATE = "20-08-2025"
		DATE = f'{self.day}-{self.month}-{self.year}'

		self.date = DATE
		self.date_ = DATE
		self.hour = f"{self.hur}:{self.minute}:{self.sec}"
		self.date__ = DATE

		self.complet_info = f"{self.date__}. {self.hour}"

	def __str__(self):
		return self.date__

	def __eq__(self,date):
		if type(date) == str:
			date = date_obj(date)
		if self.year == date.year:
			if self.month == date.month:
				if self.day == date.day:
					return True
		return False

	def __gt__(self,date):
		if type(date) == str:
			date = date_obj(date)
		if self.year > date.year:
			return True
		elif self.year < date.year:
			return False
		elif self.year == date.year:
			if self.month > date.month:
				return True
			elif self.month < date.month:
				return False
			elif self.month == date.month:
				if self.day > date.day:
					return True
		return False

	def __lt__(self,date):
		if type(date) == str:
			date = date_obj(date)
		if self.year < date.year:
			return True
		elif self.year > date.year:
			return False
		elif self.year == date.year:
			if self.month < date.month:
				return True
			elif self.month > date.month:
				return False
			elif self.month == date.month:
				if self.day < date.day:
					return True
		return False

	def __le__(self,date):
		ret = self.__eq__(date) or self.__lt__(date)
		return ret

	def __ge__(self,date):
		ret = self.__eq__(date) or self.__gt__(date)
		return ret

TODAY = date_obj

class Obj:
	def __init__(self,mypath = str(),nom_fic='davbase.dav'):

		self.name = nom_fic
		if self.name[-4:]!='.dav':
			print(self.name)
			print(info.FILEEXTENTION_ERROR)
			sys.exit(0)
		try:
			self.Data_dict = open_fic(self.name,self.this_path)
		except FileNotFoundError:
			save_fic(self.name,self.this_path)
			self.Data_dict = dict()

	def New_Base(self,name):
		print(f'Création de la base {name}')
		if name in self.Data_dict.keys():
			print(info.BASEEXISTE_ERROR)
			return 0
		dic = {'NOMINATIF':name}
		self.Data_dict[name] = dic
		save_fic(self.name,self.this_path,data = self.Data_dict)
		return dic

	def Supp(self,name):
		if name not in self.Data_dict:
			print(f"Impossible de supprimer une base '{name}' inexistante!")
			return
		self.Data_dict[f'del{name}{len(self.Data_dict)}'] = self.Data_dict[name]
		del self.Data_dict[name]
		print(f"La base {name} a très bien été supprimée!")

	def Get_Base(self,name):
		if name not in self.Data_dict:
			print(f"La Base '{name}' demandé n'existe pas!")
			return 0
		donn = self.Data_dict.get(name)
		return donn

	def Save_Base(self,dic):
		self.Data_dict[dic["NOMINATIF"]] = dic
		save_fic(self.name,self.this_path,self.Data_dict)
		return dic

	def Get_base_names(self):
		return self.Data_dict.keys()


if __name__ == "__main__":
	date_L = ["9-10-2024"]
	d1 = date_obj("9-10-2024")
	d2 = date_obj("18-10-2024")
	for dat in date_L:
		d_obj = date_obj(dat)
		if d2 > d_obj:
			print(d2,"SupEq",d_obj)