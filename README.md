
#UWSGI提供API服务案例
###一，定义APP服务程序：
格式：

import time
import ewsgi
import ework
import edb
class App(ewsgi.JrWsgiServer):
    def __init__(self):
        super().__init__()
        self.db = edb.Database('arch2018')
        self.fdm = edb.Database('fdm_new')
     def url__路由名称(参数序列)：
                方法体

if __name__.startswith('uwsgi_file_'):
    application = App()


###二，需要文件包的说明：
1，ewsgi 负责路由的解析与分发请求

2，edb 负责数据库连接，解析数据库配置，封装SQL查询



###三，配置文件的说明：
1，database.yaml 文件内容格式：
数据库别名1：

    host：xxxxxxxxx

    port： xxxx

    user： xxxx

    passwd: xxxxx

    db: xxxx



数据库别名2：

    host：xxxxxxxxx

    port： xxxx

    user： xxxx

    passwd: xxxxx

    db: xxxx



2，wsgiflow.yaml 文件内容格式：
服务程序别名xxx:
        asyncio: 5
        wsgi-file: exterior_bgw.py
        http-socket: :9093
        pidfile: /目录/exterior_bgw.pid
       daemonize: /目录/exterior_bgw.log  



###四，启动wsgi服务命令：
 uwsgi -y /目录/wsgiflow.yaml:服务程序别名xxx

