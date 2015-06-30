# -*- coding: utf-8 -*-

import MySQLdb
import prettytable


class Reporter:
	def __init__(self,config):
		self.config = config
		self.DB = MySQLdb.connect(host=config["db_host"],user=config["db_user"], passwd=config["db_passwd"],db=config["db_name"],use_unicode=1)
		self.DB.set_character_set('utf8')


	def report(self,report):
		if report == "deadlinks":
			self.deadlinks()


	def deadlinks(self):
		x = prettytable.PrettyTable(["title", "username", "deadlinks"])
		c = self.DB.cursor()
		
		query = "select channels.title, channels.username, count(*) as deadlinks from videos \
		left join channels on videos.channel_id = channels.youtube_id where available = 0 group by channel_id;"
		c.execute(query)
		
		for row in c.fetchall():
			x.add_row(row)

		print(x)
