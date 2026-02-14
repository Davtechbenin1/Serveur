#Coding:utf-8
from .connexion import local


from lib.serveur.DAV_BASE.MyData import date_obj

import calendar

import asyncio
import json
import os,time
import re
import concurrent.futures
from pathlib import Path
from fastapi import WebSocket
from psycopg2.pool import SimpleConnectionPool
from psycopg2 import sql
from datetime import datetime, timezone, timedelta
import psycopg2
import traceback
#DATABASE_URL = "postgresql://postgres:davtechbenin@localhost:8432/postgres"
DATABASE_URL = "postgresql://postgres:OjAXnBDSJNNzqnrCMgJbLvmQHFkhUwac@caboose.proxy.rlwy.net:23351/railway"

class data_main(local):
	def __init__(self):
		self.date_format = "%d-%m-%Y"
		self.date_format_new = "%d-%m-%Y. %H:%M:%S"
		local.__init__(self)
		
	
		self._local_cache = dict()
		self._lock_dict = dict()
		self.created_table = set()
		self.sc = self

		self.open_local_connexion()
#
	def open_local_connexion(self):
		self.lock = asyncio.Lock()
		self.data_lock = asyncio.Lock()
		self.table_locks = dict()

		self.Data_Table = dict()

		# Thread executor
		self.executor = concurrent.futures.ThreadPoolExecutor(
			max_workers=os.cpu_count() or 4
		)

		# PostgreSQL pool
		self.pool = SimpleConnectionPool(
			minconn=1,
			maxconn=5,
			dsn=DATABASE_URL,
			#sslmode="require"
		)
		self.created_tables = set()

		# Local folders
		self.info_dir = os.path.join(Path.home(), ".ProGest")
		os.makedirs(self.info_dir, exist_ok=True)

	# =========================
	# PostgreSQL helpers
	# =========================
	def get_conn(self):
		return self.pool.getconn()

	def put_conn(self, conn):
		self.pool.putconn(conn)

	def close_all(self):
		self.pool.closeall()
#
	def message_handler(self,msg):
		base_name = msg.get('base_name')
		dic = self.save_data(base_name,msg)
		data = dic.get('data',dict())
		if data:
			return {data.get('id'):data.get('updated_at')}
		else:
			return False

	def _get_sync_message(self,base_name, last_sync:str=None):
		try:
			if last_sync:
				last_sync_dt = datetime.fromisoformat(last_sync).astimezone(timezone.utc)
			else:
				last_sync_dt = None

			all_msgs = self._get_data(base_name,last_sync = last_sync_dt)

			return all_msgs

		except:
			Error = traceback.format_exc()
			print(Error)
			return Error


	def get_my_where(self,past_wh,base_name):
		th,part = past_wh.split('_z_o_e_')
		where = f'{base_name}_z_o_e_{part}'
		return where

	def Save_image(self,img):
		return img
	
# Gestion des dates
	def get_today(self):
		return self.normalize_date(date_obj().date__)

	def get_hour(self):
		return date_obj().hour

	def get_now(self):
		return f"{self.get_today()} .{self.get_hour()}"

	def normalize_date(self,date):
		date = date.replace("/","-")
		d,m,y = date.split('-')
		if len(y)==2:
			y = '20'+y
		elif len(y)==3:
			y = "2"+y
		elif len(y)==1:
			y = "202"+y
		if len(d)==1:
			d = '0'+d
		if len(m)==1:
			m = "0"+m
		text = f"{d}-{m}-{y}"
		return text

	def get_date_list(self,day1,day2):
		"""
			Permet d'obtenir la liste de date d'un jour 1
			à un jour 2
		"""
		d1,m1,y1 = day1.split("-")
		d2,m2,y2 = day2.split('-')

		month_list = self._month_from_years(y1,y2)
		m_y1 = f"{m1}-{y1}"
		m_y2 = f"{m2}-{y2}"
		real_month_list = self._get_month_list(m_y1,m_y2,month_list)

		all_days_liste = self._get_all_days(real_month_list)
		real_days_liste = self._get_real_days(day1,day2,all_days_liste)
		return real_days_liste

	def _month_from_years(self,y1,y2):
		y1 = int(y1)
		y2 = int(y2)
		info_liste = list()
		for y in range(y1,y2+1):
			for m in range(1,13):
				m = str(m)
				if len(m) < 2:
					m = "0"+m
				inf = f'{m}-{y}'
				info_liste.append(inf)
		return info_liste

	def _get_month_list(self,m_y1,m_y2,info_liste):
		if info_liste:
			ind1 = info_liste.index(m_y1)
			ind2 = info_liste.index(m_y2)
			month_list = info_liste[ind1:ind2+1]
			return month_list
		return list()

	def days_from_month(self,m,y):
		G_liste = calendar.monthcalendar(y,m)
		real_liste = list()
		for liste in G_liste:
			for d in liste:
				if d != 0:
					if len(str(d)) < 2:
						d = '0' + str(d)
					if len(str(m)) < 2:
						m = '0' + str(m)
					inf = f"{d}-{m}-{y}"
					real_liste.append(inf)
		return real_liste

	def _get_all_days(self,month_list):
		gene_liste = list()
		for m_y in month_list:
			m,y = m_y.split('-')
			m = int(m)
			y = int(y)
			gene_liste.extend(self.days_from_month(m,y))
		return gene_liste

	def _get_real_days(self,date1,date2,gene_days_liste):
		if gene_days_liste:
			ind1 = gene_days_liste.index(date1)
			ind2 = gene_days_liste.index(date2)
			days_liste = gene_days_liste[ind1:ind2+1]
			return days_liste
		else:
			return list()
