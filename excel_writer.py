# -*- coding: utf-8 -*-
import xlwt
import os
import StringIO


def write(excel_name, header, data):
    '''
    不落磁盘下载
    :param excel_name:
    :param header:
    :param data:
    :return:
    '''
    write_workbook = xlwt.Workbook(encoding='utf-8')
    write_sheet = write_workbook.add_sheet('Sheet 1', cell_overwrite_ok=True)
    for i in range(0, len(header)):
        write_sheet.write(0, i, header[i])
    for i in range(0, len(data)):
        for j in range(0, len(data[i])):
            write_sheet.write(i + 1, j, data[i][j])
    sio = StringIO.StringIO()
    write_workbook.save(sio)
    return excel_name, sio


def write_disk_return_full_file_name(excel_name, header, data):
    '''
        写入磁盘文件
    :param excel_name:
    :param header:
    :param data:
    :return:
    '''
    dir_ = os.path.split(os.path.realpath(__file__))[0] + '/excel/'
    full_file_name = dir_ + '%s' % excel_name
    write_workbook = xlwt.Workbook(encoding='utf-8')
    write_sheet = write_workbook.add_sheet('Sheet 1', cell_overwrite_ok=True)
    for i in range(0, len(header)):
        write_sheet.write(0, i, header[i])
    for i in range(0, len(data)):
        for j in range(0, len(data[i])):
            write_sheet.write(i + 1, j, data[i][j])
    if not os.path.exists(dir_):
        os.makedirs(dir_)
    write_workbook.save(full_file_name)
    return full_file_name
