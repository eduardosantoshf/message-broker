import selectors
import socket
import json
import xml.etree.ElementTree as element_tree
import pickle
import time

dicConsumers = {}
content = {}
protocolCon = {}


sel = selectors.DefaultSelector()

def accept(sock, mask):
    conn, addr = sock.accept() # Should be ready
    
    print('accepted', conn, 'from', addr)
    data = conn.recv(1000)

    #recebe a mensagem de registo do producer/consumer
    protocol, Midtype, topic = AckDecode(data)
    topics = []
    if(topic.count('/') > 1):
        topics = topic[1:].split('/')
    else:
        topics.append(topic[1:])
    
    #associa ao seu conn o protocolo correspondente
    protocolCon[conn] = protocol

    #se for consumer
    if(Midtype == 'MiddlewareType.CONSUMER'):
        dicConsumers[conn] = topics[len(topics)-1] #associa ao seu conn o topico subscrito 
        topico = topics[len(topics)-1]
        if(topico in content and content[topico] != None):
            message = content[topico]
            if(protocolCon[conn] == 'ProtocolType.JSON'):
                jsonT = jsonEncode(topico, message, '')
                conn.send(jsonT)
            elif(protocolCon[conn] == 'ProtocolType.Pickle'):
                pickleT = pickleEncode(topico, message, '')
                conn.send(pickleT)
            else:
                xmlT = xmlEncode(topico, message, '')
                conn.send(xmlT)
        #mensagem de SUBSCRIÇÃO de um tópico
        print('SUB FROM ', conn, ' TO TOPIC :', topic)

    sel.register(conn, selectors.EVENT_READ, read)



def read(conn, mask):
    data = conn.recv(1000)

    if data:
        #de acordo com o protocolo do producer, faz decode da mensagem
        if(protocolCon[conn] == 'ProtocolType.JSON'):
            method, topic, message = jsonDecode(data)
        elif(protocolCon[conn] == 'ProtocolType.XML'):
            method, topic, message = xmlDecode(data)
        else:
            method, topic, message = pickleDecode(data)

        topics = []
        if(topic.count('/') > 1):
            topics = topic[1:].split('/')
        else:
            topics.append(topic[1:])
        
        #no caso de ser publicação, é necessário mandar a mensagem aos consumers correspondentes
        if method == 'PUB':
            #guarda a mensagem no tópico correspondente
            for t in topics:
                content[t] = message
            #mensagem de PUBLICAÇÃO num tópico
            print('PUB IN TOPIC: ', topic, ' THE MESSAGE: ', message)
            #percorre os consumers
            for c in dicConsumers:
                #percorre os topicos pais e filhos
                for t in topics:
                    #no caso de haver match do tópico
                    if dicConsumers[c] == t:
                        #de acordo com o protocolo do consumer, faz o encode da mensagem e manda para o mesmo
                        if(protocolCon[c] == 'ProtocolType.JSON'):
                            jsonT = jsonEncode(topic, message, method)
                            c.send(jsonT)
                        elif(protocolCon[c] == 'ProtocolType.Pickle'):
                            pickleT = pickleEncode(topic, message, method)
                            c.send(pickleT)
                        else:
                            xmlT = xmlEncode(topic, message, method)
                            c.send(xmlT)
        
        #no caso do metodo ser para listagem de tópicos
        if method == 'LIST':
            method = "Listagem de tópicos: \n"
            #no caso de haver tópicos
            if(len(content) > 0):
                message = []
                #adiciona os tópicos existentes
                for topic in content:
                    message.append(topic)
                #de acordo com o protocolo do consumer, faz encode e manda a lisa de tópicos
                if(protocolCon[conn] == 'ProtocolType.JSON'):
                    jsonT = jsonEncode(topic, message, method)
                    conn.send(jsonT)
                elif(protocolCon[conn] == 'ProtocolType.Pickle'):
                    pickleT = pickleEncode(topic, message, method)
                    conn.send(pickleT)
                else:
                    xmlT = xmlEncode(topic, message, method)
                    conn.send(xmlT)
            #no caso de não haver tópicos
            else:
                message = "no topics!"
                if(protocolCon[conn] == 'ProtocolType.JSON'):
                    jsonT = jsonEncode(topic, message, method)
                    conn.send(jsonT)
                elif(protocolCon[conn] == 'ProtocolType.Pickle'):
                    pickleT = pickleEncode(topic, message, method)
                    conn.send(pickleT)
                else:
                    xmlT = xmlEncode(topic, message, method)
                    conn.send(xmlT)
        #no caso do metodo ser para o cancelamento se subscrição
        if method == 'CANCEL':
            if(conn in dicConsumers):
                del dicConsumers[conn]
            del protocolCon[conn]
            print('CANCELED SUB FROM TOPIC: ', topic, 'BY: ', conn)
    #quando o producer ou consumer se desligam
    else:
        print('closing', conn)
        if(conn in dicConsumers):
            del dicConsumers[conn]
            print('CANCELED SUB BY: ', conn)
        del protocolCon[conn] 
        sel.unregister(conn)
        conn.close()

#decode em JSON
def jsonDecode(data):
    data = data.decode('utf-8')
    jsonText = json.loads(data)
    method = jsonText['METHOD']
    topic = jsonText['TOPIC']
    message = jsonText['MESSAGE_CONTENT']
    return method, topic, message

#encode em JSON
def jsonEncode(topic, message, method):
    jsonText = {'METHOD' : method, 'TOPIC' : topic, 'MESSAGE_CONTENT': message}
    jsonText = json.dumps(jsonText)
    jsonText = jsonText.encode('utf-8')
    return jsonText

#decode em XML
def xmlDecode(data):
    decoded_xml = data.decode('utf-8')
    decoded_xml = element_tree.fromstring(decoded_xml)
    xml_txt = decoded_xml.attrib
    method = xml_txt['method']
    topic = xml_txt['topic']
    message = decoded_xml.find('message').text
    return method, topic, message

#encode em XML
def xmlEncode(topic, message, method):
    xml_txt = {'method': method, 'topic': topic, 'message': message}
    xml_txt = ('<?xml version="1.0"?><data method="%(method)s" topic="%(topic)s"><message>%(message)s</message></data>' % xml_txt)
    xml_txt = xml_txt.encode('utf-8')
    return xml_txt

#encode em Pickle
def pickleEncode(topic, message, method):
    pickleText = {'METHOD' : method, 'TOPIC' : topic, 'MESSAGE_CONTENT': message}
    pickleText = pickle.dumps(pickleText)
    return pickleText

#decode em Pickle
def pickleDecode(content):
    pickleText = pickle.loads(content)
    method = pickleText['METHOD']
    topic = pickleText['TOPIC']
    message = pickleText['MESSAGE_CONTENT']
    return method, topic, message

#decode da mensagem de registo
def AckDecode(data):
    data = data.decode('utf-8')
    jsonText = json.loads(data)
    protocol = jsonText['PROTOCOL']
    Midtype = jsonText['TYPE']
    topic = jsonText['TOPIC']
    return protocol, Midtype, topic

sock = socket.socket()
sock.bind(('', 8000))
sock.listen(100)
sel.register(sock, selectors.EVENT_READ, accept)

while True:
    events = sel.select()
    for key, mask in events:
        callback = key.data
        callback(key.fileobj, mask)