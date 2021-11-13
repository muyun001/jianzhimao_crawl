import logging

logging.basicConfig(level=logging.INFO,
                    filename='./log.txt',
                    filemode='a',
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    )

# 每次从数据库获取数据的个数
QUERY_NUMBER_CITY = 1
QUERY_NUMBER_REGION = 2500
# QUERY_NUMBER_JOB = 50

# mysql配置
MYSQL = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "123456",
    "database": "jianzhimao",
    "charset": "utf8"
}

# 网络请求状态码
RESPONSE_STATUS_CODE_NORMAL = 200
RESPONSE_STATUS_CODE_404 = 404
RESPONSE_STATUS_CODE_SERVER_WRONG = 500

# 表格中数据状态码
STATUS_NOT_CRAWL = 0  # 未抓取
STATUS_CRAWLING = 1  # 抓取中
STATUS_CRAWL_SUCCEED = 2  # 抓取成功
STATUS_CRAWL_FAILED = -1  # 抓取失败
STATUS_CRAWL_NONE = -2  # 抓取数据为空

# 表名
CITY_TABLE = "citys"  # 城市表
REGION_TABLE = "regions"  # 地区表
JOB_TABLE = "jobs"  # 兼职表

# 函数返回码
FUNC_CODE_NONE = 0  # 空值
FUNC_CODE_ERROR = -1  # 报错
FUNC_CODE_OTHER = -2  # 其他情况

# retry次数和时间配置
STOP_MAX_ATTEMPT_NUMBER = 50  # 抓取最大尝试次数
WAIT_RANDOM_MIN = 10000  # 随机等待最小时间（毫秒）
WAIT_RANDOM_MAX = 100000  # 随机等待最大时间（毫秒）
