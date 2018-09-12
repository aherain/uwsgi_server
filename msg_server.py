import datetime

import json

import edb


class customer:

    category = ''
    category_name=''
    sender=''
    sender_name=''
    title=''
    description=''
    link=''
    content=''
    status=1
    priority=1
    receivers=[]
    reminds={"data": {}, "is_all": 1}
    show_platform = 'all'
    rule = []

    def __init__(self):
    	self.db = edb.Database('arch2018')

    def setTitle(self,title:str):
        self.title=title
    def getTitle(self):
        return self.title

    def setCategory(self,category:int):
        self.category=category
    def getCategory(self):
        return self.category

    def setCategoryName(self,category_name):
        self.category_name=category_name
    def getCategoryName(self):
        return self.category_name

    def setSender(self,sender):
        self.sender=sender
    def getSender(self):
        return self.sender

    def setSenderName(self,sender_name):
        self.sender_name=sender_name
    def getSenderName(self):
        return self.sender_name

    def setDescription(self,description):
        self.description=description
    def getDescription(self):
        return self.description

    def setLink(self,linkSender):
        self.link=linkSender.link
    def getLink(self):
        return self.link

    def setContent(self,textSender):
        self.content=textSender.content
    def getContent(self):
        return self.content

    def setStatus(self,status):
        self.status=status
    def getStatus(self):
        return self.status

    def setPriority(self,priority):
        self.priority=priority
    def getPriority(self):
        return self.priority

    def setReceivers(self,receivers):
        self.receivers=receivers
    def getReceivers(self):
        return self.receivers

    def setReminds(self,reminds):
        self.reminds=reminds
    def getReminds(self):
        return self.reminds

    def setShowPlatform(self,show_platform):
        self.show_platform=show_platform
    def getShowPlatform(self):
        return self.show_platform


    def sndMsg(self):

        self.db.sys_msg.insert(
            category = self.category,
            category_name = self.category_name,
            sender = self.sender,
            sender_name = self.sender_name,
            title = self.title,
            description = self.description,
            link = self.link,
            content = json.dumps(self.content),
            status = self.status,
            priority = self.priority,
            receivers = json.dumps(self.receivers),
            reminds = json.dumps(self.reminds),
            show_platform = self.show_platform,
            create_time = datetime.datetime.now()
        )

        return True

class operate_customer(customer):

    rules = [
            ('role', '角色', 1),
            ('feature', '权限', 1),
            ('individual', '个人用户', 0),
            ('organization', '企业用户', 0),
            ('insider', '平台用户', 0),
            ('staff', '服务方', 0),
            ('consumer', '客户方', 0),
        ]

class myhuaying_customer(customer):

    rules = []



class msgSender:

    info = ''

    def send(self,info):
        pass

class linkSender(msgSender):

    def __init__(self,link):
        self.link = link
        pass


class textSender(msgSender):
    def __init__(self,content):
        self.content = content
        pass




if  __name__=="__main__":

    my_huaying_customer = myhuaying_customer()
    my_huaying_customer.setCategory(1)
    my_huaying_customer.setCategoryName('测试')
    my_huaying_customer.setSender(1)
    my_huaying_customer.setSenderName('wenquan')
    my_huaying_customer.setTitle('哈哈哈哈') #消息标题
    my_huaying_customer.setDescription('woqu') #消息提示文本
    my_huaying_customer.setStatus(2) #等待发送
    my_huaying_customer.setReceivers(["11111111"]) #手机号或账户
    my_huaying_customer.setReminds({"data": {}, "is_all": 1})
    my_huaying_customer.setShowPlatform('all')
    text_sender = textSender([{"type": 1, "value": "asjhghakdjh"}])
    link_sender = linkSender('/link')
    my_huaying_customer.setLink(link_sender) #设置连接
    a = my_huaying_customer.sndMsg()

