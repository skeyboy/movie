# coding=utf-8
import Queue
import threading
import thread

import requests
from bs4 import BeautifulSoup

import util
import MySQLdb
import time

movie_kind = []

database_host = '127.0.0.1'
database_username = 'root'
database_password = 'root'
database_dbname = 'tieba'
database_charset = 'utf8'

# 准备工作
# 打开数据库连接
db = MySQLdb.connect(database_host, database_username, database_password, database_dbname, charset=database_charset)
# 使用cursor()方法获取操作游标
cursor = db.cursor()
sql = """
CREATE TABLE IF NOT EXISTS `movie_down_url` (`id` int(32) unsigned NOT NULL AUTO_INCREMENT, `url` varchar(256) , `img_src` VARCHAR(256)  , `info` VARCHAR(256), `html_url` VARCHAR(256), `movie_cat_id` INT NOT NULL , PRIMARY KEY(`id`))
"""
cursor.execute(sql)

sql = "CREATE TABLE IF NOT EXISTS `movie_category` (`id` INT(32) unsigned NOT NULL auto_increment, `name` VARCHAR(64), `path` VARCHAR(128) UNIQUE, PRIMARY KEY(`id`))"
cursor.execute(sql)
cursor.close()
db.commit()



def parse_index(url=util.host, debug=True):
    headers = {
        # 'Host': 'blog.csdn.net',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate',
        'Referer': 'http://www.baidu.com',
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
    }
    res = requests.get(url, headers=headers)
    res.encoding = "utf-8"
    if debug is True:
        print res.encoding
        # print res.content
    return res.content


def parse_soup(url=util.host, debug=True):
    index_html = parse_index(url, debug)
    soup = BeautifulSoup(index_html, 'html.parser')
    return soup


def parse_menu(soup):
    menu = soup.find_all('div', {'class': 'menudhfun'})
    menu_items = menu[0].find_all('a')
    for item_path_and_name in menu_items:
        path = item_path_and_name.get('href')
        name = item_path_and_name.text
        print util.host + path + '\t' + name
    return menu_items


def parse_menu_item_detail(soup, layout_left_params='layout_left', debug=True):
    return soup.find('div', {'class': layout_left_params})


# u'先获取pages 解析 并获取当前页的列表数据'
def parse_menu_item_detail_page(path='/', layout_type=str('layout_left'), local_db=None, local_cursor=None, category_id=-1, debug=True):
    url = util.host + path
    soup = parse_soup(url, debug)
    if debug:
        print  "Page:" + url

    pages = soup.find('div', {'class': 'pages'})
    if pages is None:
        return
    current_page = pages.find('a', {'class': 'on'})
    next_page = pages.find_all('a', {'class': 'k'})  # 上一页 与 下一页

    if len(next_page) == 0:  # 没有抓取到 下一页 上一页
        return

    next_path = ""
    current_path = current_page.get('href')

    movie_subs = parse_menu_item_detail(soup, str(layout_type), False)
    if movie_subs is not None:
        # for detail in  movie_subs:
        lis = movie_subs.find_all('li')
        if lis is not None:

            for li in lis:

                li_a = li.find('a')
                li_img = li.find('img')
                li_span = li.find_all('span')

                img_src = li_img.get('src')
                path = li_a.get('href')

                span = ''
                if li_span is not None:
                    for span_item in li_span:
                        span += span_item.text

                if True:
                    print img_src + '\t' + span + '\t' + util.host + path
                    # print movie_subs
                movie_soup = parse_soup(util.host + path, True)
                movie_urllist = movie_soup.find_all('div', {'class': 'urllist'})
                # print "下载:"+movie_urllist
                if movie_urllist is not None:
                    for movie_urllist_item in movie_urllist:
                        movie_urls = movie_urllist_item.find_all('a')
                        # print movie_urllist_item.text
                        for movie_down in movie_urls:
                            if movie_down is not None:
                                down_url = movie_down.text

                                thunder = movie_down.find('anchor')
                                # if thunder is not None:
                                # print thunder.text
                                if "http" in down_url and "pan" not in down_url:
                                    print u"下载:" + down_url + '\n'
                                    sql = "INSERT INTO `movie_down_url`(`url`,`img_src`, `info`, `html_url`, `movie_cat_id`) VALUES ('%s','%s','%s','%s', %d)" % (down_url, img_src, span, util.host + path, category_id)
                                    try:
                                        local_cursor.execute(sql)
                                        local_db.commit()
                                    except Exception, e:
                                        # local_db.rollback()
                                        print "SQL:" + sql+"\n"+e.message

    if next_page is not None:
        next_count = len(next_page)
        next_page = next_page[next_count - 1]

        next_path = next_page.get('href')
        next_path_name = u'下一页'
        if next_path_name == next_page.text:
            parse_menu_item_detail_page(next_path, layout_type, local_db, local_cursor, category_id, debug)
    else:
        local_cursor.close()
        local_db.close()

    print 'Current Page:' + current_path + '\n' + "Next Page:" + next_path


soup = parse_soup()
menu = parse_menu(soup)
for item_path_and_name in menu:
    path = item_path_and_name.get('href')
    name = item_path_and_name.text
    print util.host + path + '\t' + name

    layout_type = str('layout_left2')


    # if path == '/':
    #     layout_type = str('layout_left')
    #     parse_menu_item_detail_page(path, layout_type, False)
    # else:
    #     parse_menu_item_detail_page(path, layout_type, False)
    local_db = MySQLdb.connect(database_host, database_username, database_password, database_dbname, charset=database_charset)
    local_cursor = local_db.cursor()

    sql = "INSERT INTO `movie_category`(`name`, `path`) VALUES ('%s', '%s')" % (name, path)
    try:
        local_cursor.execute(sql)
        local_db.commit()
    except Exception, e:
        print e.message

    sql = "SELECT * FROM `movie_category` WHERE `path`='%s'" % (path)
    local_cursor.execute(sql)
    row = local_cursor.fetchone()
    # 没有获取到数据
    if row is None:
        local_cursor.close()
        local_db.close()
        continue
    print row[0]
    category_id = row[0]
    thread.start_new_thread(parse_menu_item_detail_page,
                                (path, layout_type, local_db, local_cursor, category_id, False,))
time.sleep(60*60)

