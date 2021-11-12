import re
import time
import random
import logging
import requests
import traceback
from lxml import etree
from functools import reduce
import jianzhimao_utils as ju
from retrying import retry
from jianzhimao_utils import MysqlUtil


class ParseService(object):
    """
    解析类
    """

    @staticmethod
    def city_parse(html):
        """城市解析"""
        cu_dict = dict()
        try:
            tree = etree.HTML(html)
            a_tags = tree.xpath("//ul[@class='city_table']//a")
            for a in a_tags:
                cu_dict[a.xpath("./text()")[0]] = a.xpath("./@href")[0]
        except Exception as e:
            logging.error(f"解析城市时报错，e:{e}")
        finally:
            return cu_dict

    @staticmethod
    def region_parse(href, html):
        """地区解析"""
        rh_dict = dict()
        tree = etree.HTML(html)
        a_tags = tree.xpath('//ul[@class="box"]//li[3]/a')
        if not a_tags:
            return rh_dict
        for a in a_tags[1:]:
            h = a.xpath("./@href")
            t = a.xpath("./text()")
            if not h or not t:
                return rh_dict
            rh_dict[a.xpath("./text()")[0]] = href + a.xpath("./@href")[0]
        return rh_dict

    @staticmethod
    def job_list_parse(src, html):
        """兼职列表解析"""
        # 特殊页面：https://dali.jianzhimao.com/heqingxian_zbx_0/
        jl_list = list()
        try:
            tree = etree.HTML(html)
            a_tags = tree.xpath('//ul[@id="content_list_wrap"]')  # 直接获取li标签，内容为空
            if not a_tags:  # 如果解析不到内容，返回空值
                return jl_list

            city_href = re.findall('(http.*jianzhimao.com)', src)
            city_href = city_href[0] if city_href else ""

            url_list = a_tags[0].xpath("./li/a/@href")
            title_list = a_tags[0].xpath("./li/a/@title")
            visited_list = a_tags[0].xpath('./li/div[@class="left visited"]/span/@title')
            release_time_list = a_tags[0].xpath('./li/div[@class="left date"]/@title')
            for i in range(len(url_list)):
                jl_dict = dict()
                jl_dict["url"] = city_href + url_list[i]
                jl_dict["title"] = title_list[i]
                jl_dict["visited"] = int(visited_list[i])
                jl_dict["release_time"] = ju.format_date(release_time_list[i])
                jl_list.append(jl_dict)
        except Exception as e:
            logging.error(f"解析兼职列表时报错，e:{e}")
        finally:
            return jl_list

    @staticmethod
    def job_detail_parse(html):
        """兼职详情页解析"""
        job_detail_dict = {
            "job_price": "",
            "job_type": "",
            "recruit_num": 0,
            "work_at": "",
            "time_require": "",
            "work_time": "",
            "work_type": "",
            "at_least_weekly": "",
            "how_pay": "",
            "job_detail": "",
            "com_name": "",
            "com_info": "",
            "com_addr": "",
        }

        try:
            tree = etree.HTML(html)
            # 解析薪资和工作类型
            job_base_tag = tree.xpath('//div[@class="job_base"]')
            if not job_base_tag:  # 没解析到数据
                logging.debug("数据为空")
                return job_detail_dict
            job_price = job_base_tag[0].xpath('./span[@class="job_price"]/text()')  # 薪资
            job_detail_dict["job_price"] = job_price[0] if job_price else ""
            job_type = job_base_tag[0].xpath('./a/text()')  # 兼职类型
            job_detail_dict["job_type"] = job_type[0] if job_type else ""

            # 解析招聘人数等信息
            # recruit_num, work_at, time_require, work_type, at_least_weekly, work_time, how_pay = 0, "", "", "", "", "", ""
            li_tag = tree.xpath('//div[@class="job_content"]//li')
            if li_tag:
                for l in li_tag:
                    tit = l.xpath('./span[@class="tit"]/text()')
                    con = l.xpath('./span[@class="con"]/text()')
                    if tit and con:
                        job_detail_dict["recruit_num"] = int(con[0]) if "招聘人数" in tit[0] else job_detail_dict[
                            "recruit_num"]
                        job_detail_dict["work_at"] = con[0] if "上班地点" in tit[0] else job_detail_dict["work_at"]
                        job_detail_dict["work_type"] = con[0] if "工作种类" in tit[0] else job_detail_dict[
                            "work_type"]
                        job_detail_dict["at_least_weekly"] = con[0] if "每周至少" in tit[0] else job_detail_dict[
                            "at_least_weekly"]
                        job_detail_dict["time_require"] = con[0] if "时间要求" in tit[0] else job_detail_dict[
                            "time_require"]
                        job_detail_dict["work_time"] = con[0] if "上班时段" in tit[0] else job_detail_dict[
                            "work_time"]
                        job_detail_dict["how_pay"] = con[0] if "结算方式" in tit[0] else job_detail_dict["how_pay"]

            # 解析工作详情
            job_detail_list = tree.xpath('//*[@id="job_detail"]//text()')
            job_detail_dict["job_detail"] = reduce(lambda x, y: x + y,
                                                   job_detail_list) if job_detail_list else job_detail_dict[
                "job_detail"]

            # 解析公司信息
            # com_name, com_info, com_addr = "", "", ""
            com_tag = tree.xpath('//div[@class="company_info"]')
            if com_tag:
                t_com_name = com_tag[0].xpath('./a/text()')
                t_com_info = com_tag[0].xpath('./p[1]/text()')
                t_com_addr = com_tag[0].xpath('./p[2]/text()')
                job_detail_dict["com_name"] = t_com_name[0] if t_com_name else job_detail_dict["com_name"]
                job_detail_dict["com_info"] = t_com_info[0] if t_com_info else job_detail_dict["com_info"]
                job_detail_dict["com_addr"] = t_com_addr[0] if t_com_addr else job_detail_dict["com_addr"]
                logging.info("解析成功！")
        except Exception as e:
            logging.error(f"解析兼职详情报错,e:{e}")
        finally:
            # 将所有value值中的双引号改为单引号，以避免和sql语句中的引号冲突
            for k, v in job_detail_dict.items():
                if isinstance(v, str):
                    job_detail_dict[k] = v.strip().replace("\"", "'").replace("  ", ""). \
                        replace(" ", "").replace("\n", "").replace("\r\n", "")
            return job_detail_dict


class MysqlService(object):
    """
    mysql应用类
    """

    @staticmethod
    def create_table():
        """
        创建表
        城市表：id,城市名，城市链接，状态码（-1抓取失败，0未抓取，1抓取中，2抓取成功），抓取时间
        地区表：id，城市id，地区，区域链接，状态码，抓取时间
        兼职表：id,地区id，招聘链接，招聘标题，地区，浏览人数，发布时间，职业类型，招聘人数，上班地点，时间要求，工作种类，每周至少，上班时段，结算方式，
                基本工资，工作详情，公司名，公司介绍，公司地址，城市id，状态码（-1抓取失败，0未抓取，1抓取中，2抓取成功），抓取时间
        """
        ct_sql = f"""create table if not exists `{c.CITY_TABLE}`(
                                `id` varchar(68) not null comment "城市id",
                                `city` varchar(32) not null comment "城市名",
                                `city_href` varchar(64) not null comment "城市链接",
                                `status_code` int(1) not null default 0 comment "状态码：0未抓取，1正在抓取，2抓取成功，-1抓取失败",
                                `crawl_time` datetime not null default now() comment "抓取时间",
                                primary key (`id`))"""
        rg_sql = f"""create table if not exists `{c.REGION_TABLE}`(
                                `id` varchar(68) not null comment "区域id",
                                `city_id` varchar(64) not null comment "城市id",
                                `region` varchar(64) not null comment "地区",
                                `region_src` varchar(64) not null comment "区域链接",
                                `status_code` int(1) not null default 0 comment "状态码：0未抓取，1正在抓取，2抓取成功，-1抓取失败",
                                `crawl_time` datetime not null default now() comment "抓取时间",
                                primary key (`id`))"""
        jb_sql = f"""create table if not exists `{c.JOB_TABLE}`(
                            `id` varchar(68) not null comment "兼职id",
                            `region_id` varchar(64) comment "地区id",
    --                         `city` varchar(16) comment "城市",
                            `url` varchar(128) not null comment "招聘链接",
                            `title` varchar(128) not null comment "招聘标题",
                            `visited` int(32) comment "浏览人数",
                            `release_date` varchar(32) comment "发布日期",
                            `job_type` varchar(32) comment "职业类型",
                            `recruit_num` int(16) comment "招聘人数",
                            `work_at` varchar(64) comment "上班地点" ,
                            `time_require` varchar(64) comment "时间要求",
                            `work_type` varchar(64) comment "工作种类",
                            `at_least_weekly` varchar(64) comment "每周至少",
                            `work_time` varchar(64) comment "上班时段",
                            `how_pay` varchar(64) comment "结算方式",
                            `job_price` varchar(64) comment "基本工资",
                            `job_detail` varchar(4096) comment "工作详情",
                            `com_name` varchar(64) comment "公司名",
                            `com_info` varchar(4096) comment "公司介绍",
                            `com_addr` varchar(128) comment "公司地址",
                            `status_code` int(1) not null default 0 comment "状态码：0未抓取，1正在抓取，2抓取成功，-1抓取失败",
                            `crawl_time` datetime not null default now() comment "抓取时间",
                            primary key (`id`))"""
        MysqlUtil.create(ct_sql)
        MysqlUtil.create(rg_sql)
        MysqlUtil.create(jb_sql)
        logging.info("所有表创建成功！ or  表格已经存在，无需重复创建！")

    @staticmethod
    def insert_city(ch_dict):
        """插入城市数据"""
        for city, href in ch_dict.items():
            insert_sql = f"""insert ignore into 
                            {c.CITY_TABLE}(`id`, `city`,`city_href`) 
                            values 
                            ("{ju.hash_key(city + href)}","{city}","{href}")
                            """
            MysqlUtil.modity(insert_sql)
            print(f"插入城市数据成功！{city}:{href}")

    @staticmethod
    def insert_region(city_id, rh_dict):
        """插入区域数据"""
        for region, src in rh_dict.items():
            insert_sql = f"""insert ignore into 
                            {c.REGION_TABLE} (`id`,`city_id`,`region`,`region_src`) 
                            values 
                            ('{ju.hash_key(city_id + region)}','{city_id}','{region}','{src}')"""
            return MysqlUtil.modity(insert_sql)

    @staticmethod
    def insert_job_list(region_id, job_list):
        """插入工作列表数据"""
        job_id_list, t_data_list = list(), list()
        insert_sql = ""
        try:
            for job in job_list:
                job_id_list.append(ju.hash_key(region_id + job["url"]))  # 根据地区id和url生成job表的id
                t_data_list.append(
                    f"""'{region_id}','{job["url"]}','{job["title"]}',{job["visited"]},'{job["release_time"]}')""")

            data_list = list(map(lambda x, y: f"('{x}'" + "," + y, job_id_list, t_data_list))
            insert_data = ",".join(data_list)
            insert_sql = f"""insert ignore into 
                            {c.JOB_TABLE}(`id`,`region_id`,`url`,`title`,`visited`,`release_date`)
                            values 
                            {insert_data}
                            """
        except:
            logging.error(f"插入列表数据失败,region_id:{region_id},sql:{insert_sql}")
        return MysqlUtil.modity(insert_sql)

    @staticmethod
    def update_job_detail(job_id, job_detail_dict):
        """更新详情数据"""
        update_sql = f"""update {c.JOB_TABLE}
                                    set job_type="{job_detail_dict["job_type"]}", recruit_num={job_detail_dict["recruit_num"]},
                                        work_at="{job_detail_dict["work_at"]}",time_require="{job_detail_dict["time_require"]}",
                                        work_type="{job_detail_dict["work_type"]}",at_least_weekly="{job_detail_dict["at_least_weekly"]}",
                                        how_pay="{job_detail_dict["how_pay"]}",job_price="{job_detail_dict["job_price"]}",
                                        job_detail="{job_detail_dict["job_detail"]}",com_name="{job_detail_dict["com_name"]}",
                                        com_info="{job_detail_dict["com_info"]}",com_addr="{job_detail_dict["com_addr"]}",
                                        work_time="{job_detail_dict["work_time"]}",status_code={c.STATUS_CRAWL_SUCCEED}, 
                                        crawl_time=now()
                                    where id = "{job_id}"
                                """
        MysqlUtil.modity(update_sql)

    @staticmethod
    def update_status_code(id_tuple, status_code=c.STATUS_CRAWLING, table=c.JOB_TABLE):
        """更新表中的状态码"""
        update_sql = f"update {table} set status_code={status_code} where id in {id_tuple}"
        return MysqlUtil.modity(update_sql)

    @staticmethod
    def service_query(table=c.JOB_TABLE):
        query_city_sql = f"select id, city, city_href from {c.CITY_TABLE} where status_code = {c.STATUS_NOT_CRAWL} limit {c.QUERY_NUMBER_CITY}"
        query_region_sql = f"select id, region, region_src from {c.REGION_TABLE} where status_code = {c.STATUS_NOT_CRAWL} limit {c.QUERY_NUMBER_REGION}"
        query_job_sql = f"select id, url from {c.JOB_TABLE} where status_code = {c.STATUS_NOT_CRAWL} limit {c.QUERY_NUMBER_JOB}"
        if table == c.CITY_TABLE:
            sql = query_city_sql
        elif table == c.REGION_TABLE:
            sql = query_region_sql
        else:
            sql = query_job_sql
        return MysqlUtil.query(sql)


class CrawlService(object):
    """
    抓取类
    """
    HEADER = {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
    }

    @retry(stop_max_attempt_number=c.STOP_MAX_ATTEMPT_NUMBER, wait_random_min=c.WAIT_RANDOM_MIN,
           wait_random_max=c.WAIT_RANDOM_MAX)
    def crawl_citys(self):
        """获取所有的城市名和链接地址"""
        url = "https://www.jianzhimao.com/ctrlcity/changeCity.html"
        response = requests.get(url, headers=self.HEADER, timeout=20)
        if response.status_code != 200:
            logging.error(f"获取所有城市时出错，状态码:{response.status_code},程序退出。")
            exit()
        return ParseService.city_parse(response.text)

    @retry(stop_max_attempt_number=c.STOP_MAX_ATTEMPT_NUMBER, wait_random_min=c.WAIT_RANDOM_MIN,
           wait_random_max=c.WAIT_RANDOM_MAX)
    def crawl_regions(self, href):
        """获取所有区域，报错则返回-1"""
        response = requests.get(href, headers=self.HEADER)
        if response.status_code != c.RESPONSE_STATUS_CODE_NORMAL:
            logging.info(f"抓取以下城市的区域信息失败！ 状态码:{response.status_code}")
            return c.FUNC_CODE_ERROR
        return ParseService.region_parse(href.strip("/"), response.text)

    @retry(stop_max_attempt_number=c.STOP_MAX_ATTEMPT_NUMBER, wait_random_min=c.WAIT_RANDOM_MIN,
           wait_random_max=c.WAIT_RANDOM_MAX)
    def crawl_job_list(self, src):
        """获取工作列表数据"""
        region_job_list = list()
        for i in range(1, 12):
            page_src = src + f"index{i}.html"  # 拼接页码
            logging.info(f"正在抓取兼职列表信息，页面链接：{page_src}")
            response = requests.get(page_src, headers=self.HEADER)
            if response.status_code != c.RESPONSE_STATUS_CODE_NORMAL:
                logging.error(f"抓取'{src}'区域信息失败！ 状态码:{response.status_code}")
                return c.FUNC_CODE_ERROR
            if "抱歉，没找到你要的信息" in response.text:  # 没有数据，退出循环
                break
            job_list = ParseService.job_list_parse(src, response.text)
            if not job_list:
                logging.warning(f"目前页面解析出兼职列表为空,url:{page_src}")
                continue
            region_job_list.extend(job_list)
            time.sleep(random.random() * 5)
        return region_job_list

    @retry(stop_max_attempt_number=c.STOP_MAX_ATTEMPT_NUMBER, wait_random_min=c.WAIT_RANDOM_MIN,
           wait_random_max=c.WAIT_RANDOM_MAX)
    def crawl_job_detail(self, url):
        """获取详情页数据"""
        logging.info(f"开始抓取页面并解析.url: {url}")
        response = requests.get(url, headers=self.HEADER)
        if response.status_code != c.RESPONSE_STATUS_CODE_NORMAL:
            logging.warning(f"抓取'{url}'失败！ 状态码:{response.status_code}")
            return c.FUNC_CODE_ERROR

        return ParseService.job_detail_parse(response.text)  # 解析详情页数据


if __name__ == '__main__':
    # mysql_service = MysqlService()
    crawl_service = CrawlService()
    # parse_service = ParseService()

    # url = "https://shijiazhuang.jianzhimao.com/job/R3crN0k4M0preFk9.html"
    # response = requests.get(url, headers=crawl_service.HEADER)
    # job_detail_dict = ParseService.job_detail_parse(response.text)
    # print(job_detail_dict)

    # job_detail_list = [1, 2, 3, 4, 5]
    # print(tuple([f"{j}" for j in job_detail_list]))
    id_list = '0fbacb91a12ed0991e32576c4f34c36427d519fe'
    # id_list = ['0fbacb91a12ed0991e32576c4f34c36427d519fe', '0fbacb91a12ed0991e32576c4f34c36427d519fe']
    # if isinstance(id_list, str):
    #     id_tuple = f"('{id_list}')"
    # else:
    #     id_tuple = tuple(id_list)
    # print(tuple([].append(id_list)))
    print(f"('{id_list}')")
