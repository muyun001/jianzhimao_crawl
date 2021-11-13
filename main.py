import time
import random
import logging
import config
import threading
from service import CrawlService, MysqlService
from concurrent.futures import ThreadPoolExecutor, as_completed

crawl_service = CrawlService()
lock = threading.Lock()


def create_table():
    """创建表"""
    if MysqlService.create_table() == config.FUNC_CODE_ERROR:
        exit()


def crawl_store_city():
    """抓取城市信息"""
    cu_dict = dict()
    try:
        logging.info("开始抓取所有城市名和链接。。。")
        cu_dict = crawl_service.crawl_citys()
    except Exception as e:
        logging.error(f"抓取城市信息出错，程序即将退出！e:{e}")
        exit()

    if not cu_dict:
        logging.error("解析的城市信息为空，程序即将退出！")
        exit()

    logging.info("开始将所有城市名和链接存入数据库。。。")
    if MysqlService.insert_city(cu_dict) == config.FUNC_CODE_ERROR:
        logging.error("插入城市数据时出错，程序即将退出！")
        exit()


def crawl_store_region():
    """抓取城市和地区，并保存到数据库"""
    while True:
        logging.info("查询城市id、城市名、链接。。。")
        city_tuple = MysqlService.service_query(config.CITY_TABLE)  # 每个元组中的字段：id, city, city_href
        if not city_tuple:
            logging.info("所有城市查询完毕!")
            break
        if city_tuple == config.FUNC_CODE_ERROR:
            continue

        for c in city_tuple:
            city_id, region, region_src = c[0], c[1], c[2]

            logging.info(f"开始更新{region}地区的状态码为'抓取中'")
            if MysqlService.update_status_code(f"('{city_id}')", table=config.CITY_TABLE) == config.FUNC_CODE_ERROR:
                pass  # todo

            try:
                logging.info(f"开始根据{city_id}城市href抓取所有地区和链接...")
                rh_dict = crawl_service.crawl_regions(region_src)
            except Exception as e:
                logging.info(f"城市id:{city_id}, 现在将状态码更新为：{config.STATUS_CRAWL_FAILED}")
                if MysqlService.update_status_code(f"('{city_id}')", config.STATUS_CRAWL_FAILED,
                                                   config.CITY_TABLE) == config.FUNC_CODE_ERROR:
                    logging.error(f"更新城市数据报错！{c}, e:{e}")
                continue

            if rh_dict == config.FUNC_CODE_ERROR:
                continue

            if not rh_dict:
                logging.warning(f"没有解析到数据，region_src:{region_src}")
                if MysqlService.update_status_code(f"('{city_id}')", config.STATUS_CRAWL_FAILED,
                                                   config.CITY_TABLE) == config.FUNC_CODE_ERROR:
                    logging.error(f"更新城市数据报错！{c}")
                continue

            logging.info("开始插入地区数据。。。")
            if MysqlService.insert_region(city_id, rh_dict) == config.FUNC_CODE_ERROR:
                logging.error(f"插入地区数据错误！{rh_dict}")
                if MysqlService.update_status_code(f"('{city_id}')", config.STATUS_CRAWL_FAILED,
                                                   config.CITY_TABLE) == config.FUNC_CODE_ERROR:
                    logging.error(f"更新城市数据报错！{c}")
                continue

            logging.info(f"开始更新{region}地区的状态码为'抓取完成'")
            if MysqlService.update_status_code(f"('{city_id}')", config.STATUS_CRAWL_SUCCEED,
                                               config.CITY_TABLE) == config.FUNC_CODE_ERROR:
                logging.error(f"更新城市数据报错！{c}")
        time.sleep(random.random() * 5)


def cs_one_region_jobs(region_info):
    """抓取一个地区所有兼职的列表，并存储到数据库"""
    is_crawl_succeed = False

    region_id, region, region_src = region_info[0], region_info[1], region_info[2]
    logging.info(f"开始抓取'{region_id}'地区的所有数据，region_id:{region_id}")
    # 更新状态码
    if MysqlService.update_status_code(f"('{region_id}')", table=config.REGION_TABLE) == config.FUNC_CODE_ERROR:
        logging.error(f"更新'{region_id}'地区的状态码为'抓取中'失败！")

    try:
        """注：region_job_list：[{"url":"", "title":"", "visited":"", "release_time":""},]
            one_region_all_jobs是某地区的所有11页兼职列表的数据，[{},{},{}]"""
        one_region_all_jobs_list = crawl_service.crawl_job_list(region_src)
    except Exception as e:
        logging.info(f"抓取'{region_id}'地区的兼职列表失败！error:{e}")
        return region_id, is_crawl_succeed

    if one_region_all_jobs_list == config.FUNC_CODE_ERROR:
        return region_id, is_crawl_succeed

    if not one_region_all_jobs_list:
        logging.warning(f"'{region_id}'抓到的兼职列表数据为空！")
        return region_id, is_crawl_succeed

    # 遍历这个地区所有兼职的链接，抓取详情页
    for each_job_dict in one_region_all_jobs_list:
        """
        注：job_detail_dict ： {"job_price": "", "job_type": "", "recruit_num": 0, "work_at": "", 
                            "time_require": "","work_time": "", "work_type": "", "at_least_weekly": "", 
                            "how_pay": "","job_detail": "", "com_name": "", "com_info": "", "com_addr": ""}
        """
        job_detail_dict = crawl_service.crawl_job_detail(each_job_dict["url"])
        if job_detail_dict == config.FUNC_CODE_ERROR:
            logging.info(f"抓取{region_id}(url:{each_job_dict['url']})的兼职列表失败！")
            continue

        job_dict = {**each_job_dict, **job_detail_dict}  # 将列表页数据与详情页数据合二为一

        # lock.acquire()
        if MysqlService.insert_job(region_id, job_dict) == config.FUNC_CODE_ERROR:  # 写入数据库
            logging.error(f"数据入库失败！url:{each_job_dict['url']}")
        # lock.release()

    is_crawl_succeed = True
    return region_id, is_crawl_succeed


def run():
    create_table()  # 建表
    crawl_store_city()  # 抓取城市信息并保存
    crawl_store_region()  # 抓取地区信息并保存

    # 线程池多线程抓取兼职数据
    logging.info("从数据库获取还未抓取的地区。。。")
    region_tuple = MysqlService.service_query()  # 每个元组中的字段：id, region, region_src
    if not region_tuple:  # 如果查询到的数据为空，意为全部抓取完成，退出循环
        exit()

    with ThreadPoolExecutor(max_workers=10) as executor:
        tasks = []
        for one_region_info in region_tuple:
            tasks.append(executor.submit(cs_one_region_jobs, one_region_info))

        for future in as_completed(tasks):
            region_id, is_crawl_succeed = future.result()
            logging.info(f"抓取'{region_id}'地区的数据完成，是否抓取成功：{is_crawl_succeed}")

            # lock.acquire()  # 加线程锁
            if not is_crawl_succeed:  # 抓取失败
                u = MysqlService.update_status_code(f"('{region_id}')", config.STATUS_CRAWL_FAILED)
                if u == config.FUNC_CODE_ERROR:
                    logging.error(f"'{region_id}'地区状态码更新为-1失败！")
                continue
            # 抓取成功
            u = MysqlService.update_status_code(f"('{region_id}')", config.STATUS_CRAWL_SUCCEED)
            if u == config.FUNC_CODE_ERROR:
                logging.error(f"'{region_id}'地区状态码更新为2失败！")
            # lock.release()

        time.sleep(random.random() * 5)


if __name__ == '__main__':
    run()
