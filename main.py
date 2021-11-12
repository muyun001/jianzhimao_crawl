import time
import random
import logging
import config
from service import CrawlService, MysqlService

crawl_service = CrawlService()


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
            if MysqlService.update_status_code(f"('{city_id}')", table=c.CITY_TABLE) == config.FUNC_CODE_ERROR:
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

            if rh_dict == c.FUNC_CODE_ERROR:
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


def crawl_store_job():
    """抓取兼职列表，并存储到数据库"""
    while True:
        logging.info("从数据库获取还未抓取的地区。。。")
        region_tuple = MysqlService.service_query(config.REGION_TABLE)  # 每个元组中的字段：id, region, region_src
        if not region_tuple:  # 如果查询到的数据为空，意为全部抓取完成，退出循环
            break
        if region_tuple == config.FUNC_CODE_ERROR:  # 如果报错，休眠，继续下一轮循环
            time.sleep(random.random() * 5)
            continue

        for r_c in region_tuple:
            region_id, region, region_src = r_c[0], r_c[1], r_c[2]
            # 更新状态码
            if MysqlService.update_status_code(f"('{region_id}')", table=config.REGION_TABLE) == config.FUNC_CODE_ERROR:
                pass

            try:
                logging.info(f"开始抓取{region}(id:{region_id})的兼职列表。。。")
                # 注：region_job_list=[{"url":"", "title":"", "visited":"", "release_time":""},]
                region_job_list = crawl_service.crawl_job_list(region_src)
            except Exception as e:
                logging.info(f"抓取{region}(id:{region_id})的兼职列表失败！{e}")
                continue

            if region_job_list == config.FUNC_CODE_ERROR:
                continue

            if not region_job_list:
                logging.warning(f"{region}(id:{region_id})抓到的兼职列表数据为空！")
                continue

            logging.info(f"开始存储{region}(id:{region_id})抓到的兼职列表数据。。。")
            if MysqlService.insert_job_list(region_id, region_job_list) == config.FUNC_CODE_ERROR:
                continue

            logging.info(f"修改{region}(id:{region_id})的状态码为'抓取完成'")
            if MysqlService.update_status_code(region_id, status_code=config.STATUS_CRAWL_SUCCEED,
                                               table=config.REGION_TABLE) == config.FUNC_CODE_ERROR:
                continue
            time.sleep(random.random() * 5)
        time.sleep(random.random() * 5)


def crawl_store_jd():
    """抓取兼职详情，并保存到数据库"""
    logging.info("开始抓取兼职详情页。。。")
    while True:
        jobid_url_tuple = MysqlService.service_query()  # 每个元组中的字段：job_id, job_url
        if not jobid_url_tuple:
            logging.info(f"从{config.JOB_TABLE}数据库查询数据完毕！")
            break

        if jobid_url_tuple == config.FUNC_CODE_ERROR:
            continue

        jobid_tuple = tuple([j[0] for j in jobid_url_tuple])
        if MysqlService.update_status_code(jobid_tuple) == config.FUNC_CODE_ERROR:
            pass  # todo 错误处理

        for j in jobid_url_tuple:
            # 抓取兼职详情页,如"https://shijiazhuang.jianzhimao.com/job/R3crN0k4M0preFk9.html"
            job_id, job_url = j[0], j[1]
            job_detail_dict = crawl_service.crawl_job_detail(job_url)
            if job_detail_dict == config.FUNC_CODE_ERROR:
                MysqlService.update_status_code(f"('{job_id}')", config.STATUS_CRAWL_FAILED, config.JOB_TABLE)
                continue
            MysqlService.update_job_detail(job_id, job_detail_dict)  # 把兼职详情页信息更新到数据库,状态码也一并修改
            time.sleep(random.random())


def run():
    create_table()  # 建表
    crawl_store_city()  # 抓取城市信息并保存
    crawl_store_region()  # 抓取地区信息并保存
    crawl_store_job()  # 抓取兼职列表并保存
    crawl_store_jd()  # 抓取兼职详情并保存


if __name__ == '__main__':
    run()
