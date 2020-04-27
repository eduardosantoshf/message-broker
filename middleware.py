from enum import Enum
import socket
import sys
import fcntl
import os
import selectors
import json
import time
import xml.etree.ElementTree as element_tree
import pickle
import xml
import time

orig_fl = fcntl.fcntl(sys.stdin, fcntl.F_GETFL)
fcntl.fcntl(sys.stdin, fcntl.F_SETFL, orig_fl | os.O_NONBLOCK)

class MiddlewareType(Enum):
    CONSUMER = 1
    PRODUCER = 2

class ProtocolType(Enum):
    JSON = 1
    Pickle = 2
    XML = 3

class Queue:
    def __init__(self, topic, protocol, Mtype):
        self.topic = topic
        self.HOST = 'localhost' # Address of the host running the server 
        self.PORT = 8000 # The same port as used by the server
        self.selector = selectors.DefaultSelector()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.HOST, self.PORT))
        self.selector.register(self.socket, selectors.EVENT_READ, self.pull)
        self.protocol = protocol
        self.type = Mtype
        #mensagem inicial de registo da identidade no broker
        self.AckMessage(self.protocol, self.type, self.topic)

    def push(self, value): 
        #mensagem de publicação para o broker   
        self.message('PUB', value) 
        print(self.topic, value) #TODO remover
        pass

    def pull(self):
        #consumer bloqueado até receber data
        data = self.socket.recv(1000)
        if data:
            #decode da mensagem recebida no mecanismo de serialização correspondente
            method, topic, message = self.decode(data)
            return topic, message
        else:
            pass

    # função para enviar mensagens ao broker
    def message(self, method, data):
        #encode e envio da mensagem
        data = self.encode(self.topic, data, method)
        self.socket.send(data)
    
    #função para acknowledge inicial do producer/consumer que dá a conhecer ao broker o seu protocolo, se é consumer ou producer e qual o tópico (feito em JSON)
    def AckMessage(self, protocol, Midtype, topic):
        prot = str(protocol)
        Mtype = str(Midtype)
        jsonText = {'PROTOCOL' : prot, 'TYPE' : Mtype, 'TOPIC' : topic}
        jsonText = json.dumps(jsonText)
        jsonText = jsonText.encode('utf-8')
        self.socket.send(jsonText)

    #função que retorna a ultima mensagem do tópico que o consumer subscreveu
    def lastMessage(self):
        self.message('LAST MESSAGE', "")

    def listTopics(self):
        self.message('LIST', "")
        data = self.socket.recv(1000)
        if data:
            method, topic, message = self.decode(data)
            print(method, message)
        else:
            pass
    
    def cancelTopic(self):
        self.message('CANCEL', "")


class JSONQueue(Queue):
    def __init__(self, topic, type=MiddlewareType.CONSUMER, protocol=ProtocolType.JSON):
        super().__init__(topic, protocol, type)

    #encode em JSON
    def encode(self, topic, message, method):
        jsonText = {'METHOD' : method, 'TOPIC' : topic, 'MESSAGE_CONTENT': message}
        jsonText = json.dumps(jsonText)
        jsonText = jsonText.encode('utf-8')
        return jsonText

    #decode em JSON
    def decode(self, content):
        content = content.decode('utf-8')
        jsonText = json.loads(content)
        method = jsonText['METHOD']
        topic = jsonText['TOPIC']
        message = jsonText['MESSAGE_CONTENT']
        return method, topic, message

class XMLQueue(Queue):
    def __init__(self, topic, type=MiddlewareType.CONSUMER, protocol=ProtocolType.XML):
        super().__init__(topic, protocol, type)

    #encode em XML
    def encode(self, topic, message, method):
        xml_txt = {'method': method, 'topic': topic, 'message': message}
        xml_txt = ('<?xml version="1.0"?><data method="%(method)s" topic="%(topic)s"><message>%(message)s</message></data>' % xml_txt)
        xml_txt = xml_txt.encode('utf-8')
        return xml_txt

    #decode em XML
    def decode(self, content):
        decoded_xml = content.decode('utf-8')
        decoded_xml = element_tree.fromstring(decoded_xml)
        xml_txt = decoded_xml.attrib
        method = xml_txt['method']
        topic = xml_txt['topic']
        message = decoded_xml.find('message').text
        return method, topic, message
        
class PickleQueue(Queue):
    def __init__(self, topic, type=MiddlewareType.CONSUMER, protocol=ProtocolType.Pickle):
        super().__init__(topic, protocol, type)
    
    #encode em Pickle
    def encode(self, topic, message, method):
        pickleText = {'METHOD' : method, 'TOPIC' : topic, 'MESSAGE_CONTENT': message}
        pickleText = pickle.dumps(pickleText)
        return pickleText

    #decode em Pickle
    def decode(self, content):
        pickleText = pickle.loads(content)
        method = pickleText['METHOD']
        topic = pickleText['TOPIC']
        message = pickleText['MESSAGE_CONTENT']
        return method, topic, message


