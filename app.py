# -*- coding: utf-8 -*-
from flask import Flask, make_response, send_file, request, render_template, Response
from bk_crawler import BkCrawler
from excel_writer import write
import uuid

app = Flask(__name__)


@app.route('/')
def index():
    return render_template("index.html")


@app.route('/bk')
def bk_crawler():
    version = request.args.get('version')
    data = BkCrawler.do_crawl(version)
    data_list = map(lambda x: x.split(","), data)
    header = ['出发机场', '到达机场', '日期', '舱位名称',
              '舱位', '座位数', '成人价格', '机建燃油',
              '舱位名称', '舱位', '座位数', '成人价格', '机建燃油']
    excel_name = str(uuid.uuid1()) + '.xls'
    excel_full_name, sio = write(excel_name, header, data_list)
    response = Response(sio.getvalue(), content_type='application/vnd.ms-excel')
    response.headers["Content-disposition"] = 'attachment; filename=%s' % excel_full_name  # 如果不加上这行代码，导致下图的问题
    return response


if __name__ == '__main__':
    app.run()
