# -*- coding: utf-8 -*-

import cjson
import datetime
import time
import database
from urllib import quote
import MySQLdb

import requests
from logger import logger

BASE_URL = "https://www.okair.net/salePubAjax!queryFare.action?"
BASE_QUERY_STRING = "Tue Oct 24 2018 %s GMT 0800(中国标准时间)"

airlines = [("AEB", "SZX"), ("AEB", "TSN"), ("CAN", "TSN"), ("CKG", "CSX"), ("CKG", "NNG"), ("CKG", "TSN"),
            ("CSX", "CKG"), ("CSX", "CTU"), ("CSX", "HGH"), ("CSX", "HRB"), ("CSX", "KMG"), ("CSX", "SHE"),
            ("CSX", "SYX"), ("CSX", "TAO"), ("CSX", "TSN"), ("CSX", "URC"), ("CSX", "XIY"), ("CSX", "XMN"),
            ("CSX", "ZHA"), ("CTU", "CSX"), ("CTU", "TSN"), ("HAK", "SZX"), ("HAK", "TSN"), ("HFE", "KMG"),
            ("HGH", "CSX"), ("HGH", "SHE"), ("HRB", "CSX"), ("HRB", "TSN"), ("JHG", "KMG"), ("KHN", "TSN"),
            ("KMG", "CSX"), ("KMG", "HFE"), ("KMG", "JHG"), ("KMG", "NKG"), ("KMG", "TSN"), ("KWL", "TSN"),
            ("NGB", "SYX"), ("NGB", "TSN"), ("NKG", "KMG"), ("NNG", "CKG"), ("NNG", "SZX"), ("NNG", "TSN"),
            ("NNG", "XIY"), ("NTG", "TSN"), ("SHE", "CSX"), ("SHE", "HGH"), ("SHE", "TAO"), ("SYX", "CSX"),
            ("SYX", "NGB"), ("SYX", "TSN"), ("SYX", "ZUH"), ("SZX", "AEB"), ("SZX", "HAK"), ("SZX", "NNG"),
            ("SZX", "TSN"), ("SZX", "WUS"), ("TAO", "CSX"), ("TAO", "SHE"), ("TSN", "AEB"), ("TSN", "CAN"),
            ("TSN", "CKG"), ("TSN", "CSX"), ("TSN", "CTU"), ("TSN", "HAK"), ("TSN", "HRB"), ("TSN", "KHN"),
            ("TSN", "KMG"), ("TSN", "KWL"), ("TSN", "NGB"), ("TSN", "NNG"), ("TSN", "NTG"), ("TSN", "SYX"),
            ("TSN", "SZX"), ("TSN", "XIY"), ("TSN", "ZHA"), ("TSN", "ZUH"), ("URC", "CSX"), ("URC", "XIY"),
            ("WUS", "SZX"), ("XIY", "CSX"), ("XIY", "NNG"), ("XIY", "TSN"), ("XIY", "URC"), ("XMN", "CSX"),
            ("ZHA", "CSX"), ("ZHA", "TSN"), ("ZUH", "SYX"), ("ZUH", "TSN")]


class BkCrawler:

    def __init__(self):
        pass

    @staticmethod
    def __http_crawl(org, dst, fltDate):
        try:
            t = time.strftime("%H:%M:%S", time.localtime(time.time()))
            query_string = quote(BASE_QUERY_STRING % t)
            url = BASE_URL + query_string
            ftd = {
                "org": org,
                "dst": dst,
                "fltDate": fltDate
            }
            http_header = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'}
            http_param = {"param": cjson.encode(ftd)}
            r = requests.post(url, data=http_param, headers=http_header, timeout=30)
            data = cjson.decode(r.text)
            return data
        except:
            return {'errorCode': '9999'}

    @staticmethod
    def __parse_data(fltList):
        for flight in fltList:
            flightNo = flight['flightNoGroup']
            segmentList = flight['segmentList']
            hasLowPrice = False
            officialPrice = {}
            for segment in segmentList:
                avCabinInfoList = segment['avCabinInfo']
                for avCabinInfo in avCabinInfoList:
                    if avCabinInfo['cabinName'] == u'官方专享':
                        hasLowPrice = True
                        officialPrice['cabin'] = avCabinInfo['adClass']
                        officialPrice['cabinRemain'] = avCabinInfo['adRemain']
                        officialPrice['cabinName'] = avCabinInfo['cabinName']
                        fbcList = avCabinInfo['fbcList']
                        if len(fbcList) != 1:
                            logger.info("fbcList != 1")
                            continue
                        fbc = fbcList[0]
                        officialPrice['price'] = str(fbc['adPrice'])
                        taxFee = 0
                        for tax in fbc['taxList']:
                            if tax['taxType'] == 'ADT':
                                taxFee += int(tax['taxPrice'])
                        officialPrice['tax'] = str(taxFee)
                if not hasLowPrice:
                    # print "%s - no officialPrice price data" % flightNo
                    return flightNo, None, None

                specialOffer = {}
                lowPrice = 999999999999
                for avCabinInfo in avCabinInfoList:
                    if avCabinInfo['cabinName'] != u'官方专享':
                        fbcList = avCabinInfo['fbcList']
                        if len(fbcList) != 1:
                            logger.info("fbcList != 1")
                            continue
                        fbc = fbcList[0]
                        if int(fbc['adPrice']) < lowPrice:
                            lowPrice = int(fbc['adPrice'])
                            specialOffer['cabin'] = avCabinInfo['adClass']
                            specialOffer['cabinRemain'] = avCabinInfo['adRemain']
                            specialOffer['cabinName'] = avCabinInfo['cabinName']
                            specialOffer['price'] = str(fbc['adPrice'])
                            taxFee = 0
                            for tax in fbc['taxList']:
                                if tax['taxType'] == 'ADT':
                                    taxFee += int(tax['taxPrice'])
                            specialOffer['tax'] = str(taxFee)
                if len(specialOffer) == 0:
                    logger.info('%s - specialOffer is empty' % flightNo)
                    return flightNo, None, None
                return flightNo, officialPrice, specialOffer

    @staticmethod
    def crawl_from_db():
        db = database.Connection(host="127.0.0.1",
                                 database='crawler',
                                 user='root',
                                 password='123456')
        data = db.query("select * from bk")
        data = map(lambda x: x['data'], data)
        db.close()
        return data

    @staticmethod
    def real_time_crawl():
        start = time.time()
        dates = []
        now = datetime.datetime.now()
        date_end_rage = 18
        for i in range(1, date_end_rage):
            delta = datetime.timedelta(days=i)
            n_days = now + delta
            dates.append(n_days.strftime("%Y%m%d"))
        result = []
        for airline in airlines:
            for fltDate in dates:
                time.sleep(0.1)
                data = BkCrawler.__http_crawl(airline[0], airline[1], fltDate)
                if 'errorCode' not in data or data['errorCode'] != '00':
                    logger.info(
                        '%s - %s - %s execute failed, code:%s' % (airline[0], airline[1], fltDate, data['errorCode']))
                    continue
                fltList = data['fltList']
                flightNo, officialPrice, specialOffer = BkCrawler.__parse_data(fltList)
                if officialPrice is None:
                    logger.info("%s - %s - %s - %s - is no officialPrice" % (airline[0], airline[1], fltDate, flightNo))
                    continue
                if int(officialPrice['price']) >= int(specialOffer['price']):
                    logger.info("%s - %s - %s - %s - price same" % (airline[0], airline[1], fltDate, flightNo))
                    continue
                result_str = ",".join([airline[0], airline[1], fltDate,
                                       officialPrice['cabinName'], officialPrice['cabin'],
                                       officialPrice['cabinRemain'], officialPrice['price'], officialPrice['tax'],
                                       specialOffer['cabinName'], specialOffer['cabin'], specialOffer['cabinRemain'],
                                       specialOffer['price'], specialOffer['tax']])

                logger.info(result_str)
                result.append(result_str)
        end = time.time()
        logger.info("bk crawler %s , cost:%s" % (str(date_end_rage), str(end - start)))
        return result

    @staticmethod
    def store_to_db(data):
        db = database.Connection(host="127.0.0.1",
                                 database='crawler',
                                 user='root',
                                 password='123456')
        affect = db.execute_rowcount("DELETE FROM bk")
        logger.info("delete bk affect :%s" % str(affect))
        insert_list_data = map(lambda x: '("%s")' % MySQLdb.escape_string(x.encode("utf8")), data)
        sql = "INSERT INTO bk(data) VALUES %s" % ','.join(insert_list_data)
        affect = db.execute_rowcount(sql)
        logger.info("insert bk affect :%s" % str(affect))
        db.close()

    @staticmethod
    def do_crawl(version):
        if version == 'cache':
            data = BkCrawler.crawl_from_db()
            if data is not None and len(data) > 0:
                return data
        data = BkCrawler.real_time_crawl()
        BkCrawler.store_to_db(data)
        return data
