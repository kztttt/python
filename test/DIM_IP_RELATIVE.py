#!/usr/bin/env python
# -*-coding:utf-8 -*-


from env_path import *
from settings import *
from hqltools import *
from log4py import logger
import httplib, json, sys, os.path, os, shutil,threading,Queue
from os.path import basename
import time
import sys
reload(sys)
sys.setdefaultencoding('utf8')

#===============================================================================
# 程序开始执行
#===============================================================================
name =  os.path.basename(sys.argv[0]).rstrip('.py')
dates=sys.argv[1]

#===============================================================================
# 定义源表
# 该表为目增量表, 每天覆盖写入供IP查询使用
#===============================================================================
s_dim_new_ipaddress      =    'dim_new_ipaddress'
#===============================================================================
# 定义hive目标表
#===============================================================================
t_dim_ip_relative        =    'dim_ip_relative'


#===============================================================================
# 定义公共变量
#===============================================================================
keys          =    ('country', 'area', 'region', 'city', 'county', 'isp', 'ip')
hadoopfs      =    '/data/ocetl/app/hadoop/bin/hadoop fs '
temp_path     =    os.path.join(PRJ_HOME,'temp')
src_path      =    os.path.join(temp_path,s_dim_new_ipaddress)
dst_path      =    os.path.join(temp_path,t_dim_ip_relative)

# 定义hive库
hql_list = ["use " + hive_db]

def ignoreHiddenFiles(ff):
    if(ff.startswith('.', 0, 1)):
        return False
    else:
        return True


def unload_ip_address():
    '''
    unload ip info from hive table
    @return: file  返回不带路径的文件名列表
    '''
    logger.info("unload ip address from %s", s_dim_new_ipaddress)
    hsql = '''
    insert overwrite directory '/tmp/%(s_dim_new_ipaddress)s' select ip_address from %(s_dim_new_ipaddress)s
    ''' % globals()
    hql_list.append('set hive.exec.compress.output=false')
    hql_list.append(hsql)
    HiveExe(hql_list, name, dates)
    hql_list.pop()
    cmd = '%(hadoopfs)s -copyToLocal -ignoreCrc /tmp/%(s_dim_new_ipaddress)s %(temp_path)s' % globals()
    logger.info(cmd)
    os.system(cmd)
    
    file_list = os.listdir(src_path)
    file = filter(ignoreHiddenFiles, file_list)
    logger.info("ip files are %s", file)
    return file


def loadDataToHive():
    # 创建目标临时表
    cmd = '%(hadoopfs)s -moveFromLocal %(dst_path)s /tmp' % globals()
    logger.info(cmd)
    os.system(cmd)

    hsql = '''
    create table if not exists %(t_dim_ip_relative)s (
    country    string,
    area       string,
    province   string,
    city       string,
    county     string,
    isp        string,
    ip         string
    )
    location '%(tmp_path)s/%(t_dim_ip_relative)s'
    ''' % globals()
    hql_list.append(hsql)
    HiveExe(hql_list, name, dates)
    hql_list.pop()
 
    hsql = '''
    load data inpath '/tmp/%(t_dim_ip_relative)s' overwrite into table %(t_dim_ip_relative)s
    ''' % globals()
    hql_list.append(hsql)
    HiveExe(hql_list, name, dates)
    hql_list.pop()


def cleanEnv():
    shutil.rmtree(src_path, True)
    shutil.rmtree(dst_path, True)
    os.makedirs(dst_path)
    cmd = '%(hadoopfs)s -rm -r -f /tmp/%(s_dim_new_ipaddress)s' % globals()
    logger.info(cmd)
    os.system(cmd)


class TaoBaoRest():
    '''
    Purpose: 连接淘宝ip数据库, 返回ip所对应的归属信息 , 国家/区域/省(直辖市)/地市(直辖市)/县/运营商

    '''

    def __init__(self):
        self.conn = httplib.HTTPConnection('ip.taobao.com', 80)

    def reConn(self):
        self.conn.close()
        self.conn.connect()

    def runRest(self, ip):
        self.conn.request('GET', '/service/getIpInfo.php?ip=' + ip)
        self.resp = self.conn.getresponse()
        r_data = self.resp.read()
        return r_data

    def closeConn(self):
        self.conn.close()

    def getRespStatus(self):
        return self.resp.status,self.resp.reason

    def __del__(self):
        self.conn.close()


def splitFile(srcFile, linesPerFile):
    '''
    Purpose: 把hive导出来的IP文件切割成多个小文件批量执行, 减轻服务器压力的同时提高应用执行效率
    @param srcFile: 被分隔的文件, str类型;
    @param linesPerFile: 分隔后每个小文件包含的行数, int类型; ex: 300记录/文件
    '''
    desFile=[]
    line_cnt = 1
    file_cnt = 1
    s_file = os.path.join(src_path, srcFile)
    tfile = s_file + '__' + str(file_cnt)
    fd  = open(s_file, 'r')
    fdx = open(tfile, 'w')
    for line in fd:
        if(line_cnt <= linesPerFile):
            if(len(line) > 0 and line[0].isdigit()):
                logger.debug('File_cnt %d %d ====== %s', file_cnt, line_cnt, line.rstrip('\n'))
                fdx.write(line)
            else:
                logger.error('skip %s', line.rstrip('\n'))
        else:
            fdx.close()
            desFile.append(basename(tfile))
            del fdx
            file_cnt += 1
            tfile = s_file + '__' + str(file_cnt)
            fdx = open(tfile, 'w')
            line_cnt = 1
            if(len(line) > 0 and line[0].isdigit()):
                logger.debug('File_cnt %d %d ====== %s', file_cnt, line_cnt, line.rstrip('\n'))
                fdx.write(line)
            else:
                logger.error('skip %s', line.rstrip('\n'))
        line_cnt += 1
    fd.close()
    fdx.close()
    desFile.append(basename(tfile))
    return desFile


def run(srcFile):
    '''
    Purpose: 执行函数
    
    @param srcFile: 源文件
    '''
    s_file = os.path.join(src_path, srcFile)
    stime = time.strftime('%Y%m%d%H%M%S')
    t_file = os.path.join(dst_path, srcFile + '_' + stime)
    logger.info(t_file)
    fd_src = open(s_file, 'r')
    fd_dst = open(t_file, 'w')
    rest = TaoBaoRest()
    for ip in fd_src:
        ip = ip.translate(None,'\r\n')
        flag = False
        while (not flag):
            try:
                data = rest.runRest(ip)
                logger.info('%s ====== %s', ip, data)
                parser = json.loads(data)
                flag = True
            except Exception as e:
                flag = False
                logger.error('Occur exception:\n%s', e)
                logger.error('%s <------> %s', ip, data)
                rest.reConn()
        Ret = parser['code']
        if (0 == Ret):
            data = parser['data']
            for k in keys:
                fd_dst.write(data[k].encode('UTF-8') + '\001')
            fd_dst.write('\n')
        else:
            logger.error('Ret = %d , discard ip: %s', Ret, ip)
#         rest.reConn()
    fd_src.close()
    fd_dst.close()
    del rest


if __name__ == '__main__':
    try:
        Start(name, dates)

        cleanEnv()
        thread_pool = Queue.Queue(-1)
        thread_rec = []
        rkfile = []
        f_addr = unload_ip_address()
        for f in f_addr:
            rkfile.extend(splitFile(f, 300))

        # 生成线程池
        for f in rkfile:
            thread = threading.Thread(None, target=run, args=(f,))
            thread_rec.append(thread)
            thread_pool.put(thread)

        # 使用不超过10个线程处理, 总线程数量 = 10 + 已激活的线程数量
        logger.info('threading.enumerate() is %s', threading.enumerate())
        totalThreads = threading.activeCount() + 10
        while(thread_pool.qsize() > 0):
            if(threading.activeCount() < totalThreads):
                thread_pool.get().start()

        # 等待所有线程完成
        for tt in thread_rec:
            tt.join()

        loadDataToHive()

        End(name, dates)
    except Exception, e:
        Except(name, dates, e)
