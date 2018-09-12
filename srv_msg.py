# -- coding: utf-8 --
import time
import ewsgi
import ework
import edb
from msg_server import *
import json
class App(ewsgi.JrWsgiServer):

    def __init__(self):
        super().__init__()
        self.db = edb.Database('arch2018')
        self.fdm = edb.Database('fdm_new')
        self.producer_id = 7079

        return

    #来自建华的调去片方用户接口
    def get_project_app_users(self, pid:str, cooperator_id:int )->list:
        '''
        获取项目片方关联的用户ID，用以接收结算消息
        参数：
            pid : 项目编号
            cooperator_id : 项目片方ID，即合同甲方ID，bpm 数据库 bpm_issue_tasks 表 filmSide_uid 字段值

            self.db = edb.Database('arch2018')
            self.fdm = edb.Database('fdm')
        '''
        users = self.fdm.fdm_project_user.where(pid=pid).select()
        uids = []
        acct = []
        for u in users:
            cids = [v["cooperator_id"] for v in u["cooperators"]]
            if cooperator_id in cids:
                uids.append(u["user_id"])

        if len(uids) > 0:
            rs = self.db.acct_users.where_in(user_id = tuple(uids)).select("account")
            acct = [v["account"] for v in rs]

        return acct

    def url__send_msg(self, task_type: int, pid: str, name:str, cooperator_id:int, push_msg:str):
        """
        获取options变量
        
        返回值举例
            [option] -> [ API/SQL值, 英文值, 中文值 ]
        """
        # return '%s_%s_%s_%s_%s' % (task_type, pid, cooperator_id, name, push_msg)
        # 《xxx》可结算分账款通知
        # 《xxx》发行收入通知
        map_struct = {1: {'category': 4, 'categoryName': '结算消息', 'senderName': '推送服务', 'title': "《%s》发行收入通知" % name},
                     2: {'category': 4, 'categoryName': '结算消息', 'senderName': '推送服务', 'title': "《%s》可结算分账款通知" % name}}
        if task_type not in map_struct:
            return -1

        my_huaying_customer = myhuaying_customer()
        my_huaying_customer.setCategory(map_struct[task_type]['category'])
        my_huaying_customer.setCategoryName(map_struct[task_type]['categoryName'])
        my_huaying_customer.setSender(1)
        my_huaying_customer.setSenderName('结算推送服务')
        my_huaying_customer.setTitle(map_struct[task_type]['title'])  # 消息标题
        my_huaying_customer.setDescription(push_msg)  # 消息提示文本
        my_huaying_customer.setStatus(2)  # 等待发送
        my_huaying_customer.setReceivers(self.get_project_app_users(pid, cooperator_id))  # 手机号或账户
        my_huaying_customer.setReminds({"data": {}, "is_all": 1})
        my_huaying_customer.setShowPlatform('all')
        # text_sender = textSender([{"type": 1, "value": "asjhghakdjh"}])
        link = ('/settlement_c/releaseIncome/{}/{}'.format(pid, cooperator_id)) if task_type == 1 else (
        '/settlement_c/pay/{}/{}'.format(pid, cooperator_id)),
        my_huaying_customer.setLink(linkSender(link))  # 设置连接

        try:
            my_huaying_customer.sndMsg()
        except:
            return -1 #数据写入失败

        return 1

if __name__.startswith('uwsgi_file_'):
    application = App()
