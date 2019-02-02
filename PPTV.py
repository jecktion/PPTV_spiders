# -*- coding: utf-8 -*-
# 此程序用来抓取 的数据
import requests
import time
import re
from multiprocessing.dummy import Pool
import csv
import random
from fake_useragent import UserAgent, FakeUserAgentError
from save_data import database

class Spider(object):
	def __init__(self):
		try:
			self.ua = UserAgent(use_cache_server=False).random
		except FakeUserAgentError:
			pass
		# self.date = '2000-10-01'
		# self.limit = 500000
		self.db = database()

	def get_headers(self):
		try:
			user_agent = self.ua.chrome
			headers = {'Host': 'apicdn.sc.pptv.com', 'Connection': 'keep-alive',
					   'User-Agent': user_agent,
					   'Referer': 'http://v.pptv.com/show/NnJb2UGnF1W4Np4.html?spm=pc_search_web.search.result.1',
					   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
					   'Accept-Encoding': 'gzip, deflate, br',
					   'Accept-Language': 'zh-CN,zh;q=0.8'
					   }
		except AttributeError:
			user_agents = ['Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20130406 Firefox/23.0',
						   'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:18.0) Gecko/20100101 Firefox/18.0',
						   'IBM WebExplorer /v0.94', 'Galaxy/1.0 [en] (Mac OS X 10.5.6; U; en)',
						   'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
						   'Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14',
						   'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0; TheWorld)',
						   'Opera/9.52 (Windows NT 5.0; U; en)',
						   'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.2pre) Gecko/2008071405 GranParadiso/3.0.2pre',
						   'Mozilla/5.0 (Windows; U; Windows NT 5.2; en-US) AppleWebKit/534.3 (KHTML, like Gecko) Chrome/6.0.458.0 Safari/534.3',
						   'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/4.0.211.4 Safari/532.0',
						   'Opera/9.80 (Windows NT 5.1; U; ru) Presto/2.7.39 Version/11.00']
			user_agent = random.choice(user_agents)
			headers = {'Host': 'apicdn.sc.pptv.com', 'Connection': 'keep-alive',
					   'User-Agent': user_agent,
					   'Referer': 'http://v.pptv.com/show/NnJb2UGnF1W4Np4.html?spm=pc_search_web.search.result.1',
					   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
					   'Accept-Encoding': 'gzip, deflate, br',
					   'Accept-Language': 'zh-CN,zh;q=0.8'
					   }
		return headers
	
	def p_time(self, stmp):  # 将时间戳转化为时间
		stmp = float(str(stmp)[:10])
		timeArray = time.localtime(stmp)
		otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
		return otherStyleTime
	
	def replace(self, x):
		# 将其余标签剔除
		removeExtraTag = re.compile('<.*?>', re.S)
		x = re.sub(removeExtraTag, "", x)
		x = re.sub(re.compile('[\r\n]'),' ',x)
		x = re.sub(re.compile('\s{2,}'),' ',x)
		return x.strip()
	
	def GetProxies(self):
		# 代理服务器
		proxyHost = "http-dyn.abuyun.com"
		proxyPort = "9020"
		# 代理隧道验证信息
		proxyUser = "HK847SP62Z59N54D"
		proxyPass = "C0604DD40C0DD358"
		proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
			"host": proxyHost,
			"port": proxyPort,
			"user": proxyUser,
			"pass": proxyPass,
		}
		proxies = {
			"http": proxyMeta,
			"https": proxyMeta,
		}
		return proxies

	def get_detail_page(self, ss):  # 获取某一页的所有评论
		film_url, vod, product_number, plat_number, page = ss
		print 'page:',page
		url = "http://apicdn.sc.pptv.com/sc/v4/pplive/ref/vod_%s/feed/list" % vod
		querystring = {"appplt": "web", "action": "1", "pn": str(page - 1), "ps": "20"}
		retry = 10
		while 1:
			try:
				results = []
				text = requests.get(url, headers=self.get_headers(),proxies=self.GetProxies(),  timeout=10,
				                    params=querystring).json()
				items = text['data']['page_list']
				last_modify_date = self.p_time(time.time())
				for item in items:
					try:
						nick_name = item['user']['nick_name']
					except:
						nick_name = ''
					try:
						tmp1 = self.p_time(item['create_time'])
						cmt_date = tmp1.split()[0]
						# if cmt_date < self.date:
						# 	continue
						cmt_time = tmp1
					except:
						cmt_date = ''
						cmt_time = ''
					try:
						comments = self.replace(item['content'])
					except:
						comments = ''
					try:
						like_cnt = str(item['up_ct'])
					except:
						like_cnt = '0'
					try:
						cmt_reply_cnt = str(item['reply_ct'])
					except:
						cmt_reply_cnt = '0'
					long_comment = '0'
					source_url = film_url
					tmp = [product_number, plat_number, nick_name, cmt_date, cmt_time, comments, like_cnt,
					       cmt_reply_cnt, long_comment, last_modify_date, source_url]
					print '|'.join(tmp)
					results.append([x.encode('gbk', 'ignore') for x in tmp])
				return results
			except Exception as e:
				retry -= 1
				if retry == 0:
					print e
					return None
				else:
					continue
	
	def get_total_page(self, vod):  # 获取总页数
		url = "http://apicdn.sc.pptv.com/sc/v4/pplive/ref/vod_%s/feed/list" % vod
		querystring = {"appplt": "web", "action": "1", "pn": '0', "ps": "20"}
		retry = 5
		while 1:
			try:
				text = requests.get(url, headers=self.get_headers(),proxies=self.GetProxies(), timeout=10,
				                    params=querystring).json()
				total_pages = str(text['data']['page_count'])
				return total_pages
			except Exception as e:
				retry -= 1
				if retry == 0:
					print e
					return None
				else:
					continue
	
	def get_vod(self, film_url):  # 获取电影的VOD
		retry = 5
		while 1:
			try:
				headers = self.get_headers()
				headers['host'] = 'v.pptv.com'
				text = requests.get(film_url, headers=headers, proxies=self.GetProxies(), timeout=10).text
				p0 = re.compile(u'var webcfg = \{"id":(\d+?),')
				vod = re.findall(p0, text)[0]
				return vod
			except:
				retry -= 1
				if retry == 0:
					return None
				else:
					continue
	
	def save_sql(self, table_name,items):  # 保存到sql
		all = len(items)
		print 'all:',all
		results = []
		for i in items:
			try:
				t = [x.decode('gbk', 'ignore') for x in i]
				dict_item = {'product_number': t[0],
				             'plat_number': t[1],
				             'nick_name': t[2],
				             'cmt_date': t[3],
				             'cmt_time': t[4],
				             'comments': t[5],
				             'like_cnt': t[6],
				             'cmt_reply_cnt': t[7],
				             'long_comment': t[8],
				             'last_modify_date': t[9],
				             'src_url': t[10]}
				results.append(dict_item)
			except:
				continue
		for item in results:
			try:
				self.db.add(table_name, item)
			except:
				continue
	
	def get_all_comments(self, film_url, product_number, plat_number):  # 获取所有评论
		print product_number
		vod = self.get_vod(film_url)
		if vod is None:
			return None
		pagenums = self.get_total_page(vod)
		if pagenums is None:
			return None
		else:
			ss = []
			# if int(pagenums) > self.limit / 20:
			# 	pagenums = self.limit / 20
			print u'pagenums:%s' % pagenums
			for page in range(1, int(pagenums) + 1):
				ss.append([film_url, vod, product_number, plat_number, page])
			pool = Pool(5)
			items = pool.map(self.get_detail_page, ss)
			pool.close()
			pool.join()
			mm = []
			for item in items:
				if item is not None:
					mm.extend(item)

			with open('data_comment.csv', 'a') as f:
				writer = csv.writer(f, lineterminator='\n')
				writer.writerows(mm)

			# print u'%s 开始录入数据库' % product_number
			# self.save_sql('T_COMMENTS_PUB_MOVIE',mm) # 手动修改需要录入的库的名称
			# print u'%s 录入数据库完毕' % product_number


if __name__ == "__main__":
	spider = Spider()
	s = []
	with open('new_data.csv') as f:
		tmp = csv.reader(f)
		for i in tmp:
			if 'http' in i[2]:
				s.append([i[2], i[0]])
	for j in s:
		print j[1],j[0]
		spider.get_all_comments(j[0], j[1], 'P06')
	spider.db.db.close()

