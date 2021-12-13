import requests
import pymysql
import logging
import config
from hashlib import sha1
import datetime
from DBUtils.PooledDB import PooledDB, SharedDBConnection
from DBUtils.PersistentDB import PersistentDB, PersistentDBError, NotSupportedError

# connection = pymysql.connect(**config.MYSQL)
# cursor = connection.cursor()

poolDB = PooledDB(
    # 指定数据库连接驱动
    creator=pymysql,
    # 连接池允许的最大连接数,0和None表示没有限制
    maxconnections=10,
    # 初始化时,连接池至少创建的空闲连接,0表示不创建
    mincached=2,
    # 连接池中空闲的最多连接数,0和None表示没有限制
    maxcached=5,
    # 连接池中最多共享的连接数量,0和None表示全部共享
    maxshared=3,
    # 连接池中如果没有可用共享连接后,是否阻塞等待,True表示等等,
    # False表示不等待然后报错
    blocking=True,
    # 开始会话前执行的命令列表
    setsession=[],
    # ping Mysql服务器检查服务是否可用
    ping=0,
    **config.MYSQL,
)


def wechat_remind(title, content):
    """微信提醒"""
    url = "http://pushplus.hxtrip.com/send"
    params = {
        "token": "e71cb823d3564e939a79a06ffc6e9114",
        "title": title,
        "content": content,
    }

    response = requests.get(url, params=params)
    return response.text


def format_date(date_str):
    """转换时间"""
    now_date = datetime.datetime.now()
    if date_str == "刚刚" or "前" in date_str:
        return now_date.strftime("%Y-%m-%d")
    if date_str == "昨天":
        return (now_date + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
    if date_str == "前天":
        return (now_date + datetime.timedelta(days=-2)).strftime("%Y-%m-%d")
    return date_str


def hash_key(data):
    """生成字符串的hash值"""
    return sha1(data.encode("utf8")).hexdigest()


class MysqlUtil(object):

    @staticmethod
    def create(sql):
        """创建表格"""
        try:
            connection = poolDB.connection()
            cursor = connection.cursor()
            cursor.execute(sql)
        except Exception as e:
            logging.error(f"创建表格报错，e:{e},sql:{sql}")
            connection.rollback()

    @staticmethod
    def modity(sql):
        """插入数据/修改数据"""
        try:
            connection = poolDB.connection()
            cursor = connection.cursor()
            cursor.execute(sql)
            connection.commit()
        except Exception as e:
            logging.error(f"插入数据/修改数据报错，e:{e},sql:{sql}")
            connection.rollback()

    @staticmethod
    def query(sql):
        """查询数据"""
        try:
            connection = poolDB.connection()
            cursor = connection.cursor()
            cursor.execute(sql)
            return cursor.fetchall()
        except Exception as e:
            logging.error(f"查询数据报错，e:{e},sql:{sql}")
            connection.rollback()
