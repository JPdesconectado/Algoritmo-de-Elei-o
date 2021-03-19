import socket 
import threading 
import time
import stopit

HOST = "127.0.0.1"  
IDPORT = 3000
PartnersPORTs = [1000, 2000, 4000, 5000, 6000]

class lastMessage(object):
    def __init__(self, message):
        self._message = message

class newCoordenator(object):
    def __init__(self, isCoordenator):
        self._isCoordenator = isCoordenator

class coordenatorPORT(object):
    def __init__(self, coordenatorPORT):
        self._coordenatorPORT = coordenatorPORT

class endCoordenator(object):
    def __init__(self, rip):
        self._rip = rip

def serverThread():
    addr = (HOST, IDPORT) 
    serv_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serv_socket.bind(addr) 
    serv_socket.listen()
    serv_socket.settimeout(3)
    tam = 0
    while True:
        try:
            con, port = serv_socket.accept() 
        except socket.timeout:
            if (end._rip and lastmsg._message != "ELEIÇÃO"):
                greater = 0
                for port in PartnersPORTs:
                    if (IDPORT > port):
                        greater = 1
                    else:
                        greater = 0
                if (greater == 1):
                    setCoordenator()
        except:
            raise
        else:
            try:
                while True:
                    msg = con.recv(1024) 
                    if not msg: break

                    getMsg, getPort = infos(msg)

                    if (getMsg == "PONG"):
                        lastmsg._message = getMsg
                        print("Conexão com:" + getPort + " Mensagem:", getMsg)

                    if (getMsg == "PING"):
                        lastmsg._message = getMsg
                        print("Conexão com:" +  getPort + " Mensagem:", getMsg)
                        th = threading.Thread(target = responseCoordenatorThread(int(getPort)))
                        th.start()

                    if (getMsg == "ELEIÇÃO"):
                        end._rip = True
                        lastmsg._message = getMsg
                        print("Conexão com:" +  getPort + " Mensagem:", getMsg)
                        responseEleicao(int(getPort))

                    if(getMsg == "OK"):
                        lastmsg._message = getMsg
                        end._rip = True
                        print("Conexão com:" +  getPort + " Mensagem:", getMsg)
                        if (IDPORT > int(getPort)):
                            mensagem = "ELEIÇÃO:" + str(IDPORT)
                            for partners in PartnersPORTs:
                                if (partners > IDPORT):
                                    th = threading.Thread(target = newCoordenatorThread(partners, mensagem))
                                    th.start()   
                        else:
                            responseEleicao(int(getPort))
                            break    

                    if (getMsg == "COORDENADOR"):
                        lastmsg._message = getMsg
                        if IDPORT == getPort:
                            break
                        else:
                            if tam == 0:
                                print("O processo:", getPort, "é o novo Coordenador.")
                                ctport._coordenatorPORT = int(getPort)
                                end._rip = False
                                tam = 1
                                break
            finally:
                con.close()        

        
@stopit.threading_timeoutable(default="endcon")
def responsetimeout(s):
    try:
        return s.accept()
    except:
        responsetimeout(s)    

def infos(msg):
    var = msg.decode().split(":")
    tam = len(var)
    getMsg = var[0]
    getPort = var[1]
    return getMsg, getPort

def sendEleicao():
    mensagem = "ELEIÇÃO:" + str(IDPORT)
    for partners in PartnersPORTs:
        th = threading.Thread(target = newCoordenatorThread(partners, mensagem))
        th.start()       

def responseEleicao(port):
    mensagem = "OK:" + str(IDPORT)
    th = threading.Thread(target = newCoordenatorThread(port, mensagem))
    th.start()

def setCoordenator():
    coordenator._isCoordenator = True
    ctport._coordenatorPORT = IDPORT
    mensagem = "COORDENADOR:" + str(IDPORT)
    for partners in PartnersPORTs:
        th = threading.Thread(target = newCoordenatorThread(partners, mensagem))
        th.start()    

@stopit.threading_timeoutable(default="endcon")
def test_con(s, PORT):
    try:
       return s.connect((HOST, PORT))
    except:
        test_con(s, PORT)

def checkCoordenatorThread(PORT):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        result = test_con(s, PORT, timeout=10)
        if result == "endcon" and not end._rip:
            print("TIMEOUT")
            end._rip = True
            sendEleicao()
            return
        if (end._rip):
            return
        else:    
            mensagem = "PING:" + str(IDPORT)
            s.sendall(mensagem.encode('utf-8')) 

def newCoordenatorThread(PORT, mensagem):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        test_con(s, PORT)
        s.sendall(mensagem.encode('utf-8'))

def responseCoordenatorThread(PORT):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        test_con(s, PORT)
        mensagem = "PONG:" + str(IDPORT)
        s.sendall(mensagem.encode('utf-8'))


end = endCoordenator(False)
coordenator = newCoordenator(False)
lastmsg = lastMessage("")
ctport = coordenatorPORT(3000)

def Main(): 
    tw = threading.Thread(target=serverThread)
    tw.start()
    coordenator._isCoordenator = False
    lastmsg._message = ""
    ctport._coordenatorPORT = 7000
    while (True):
        if (not coordenator._isCoordenator and not end._rip):
            th = threading.Thread(target = checkCoordenatorThread(ctport._coordenatorPORT))
            th.start()
            time.sleep(5)

if __name__ == '__main__': 
    Main() 