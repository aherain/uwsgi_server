import edb
import ewsgi
import srv_delivery
import json

import srv_keys


class App(ewsgi.JrWsgiServer):

    def __init__(self):
        super().__init__()
        self.db = edb.Database('arch2018')
        self.fdm = edb.Database('fdm')
        self.producer_id = 7079

    def url__project__get_project_app_users(self, pid:str, cooperator_id:int )->list:
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
            if cooperator_id in cids :
                uids.append(u["user_id"])

        if len(uids) > 0 :
            rs = self.db.acct_users.where_in(user_id = tuple(uids)).select("account")
            acct = [v["account"] for v in rs]

        return acct


if __name__.startswith('uwsgi_file_'):
    application = App()


