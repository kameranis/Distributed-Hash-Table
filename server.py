#!/usr/bin/env python

import socket, select, Queue, re
import sys
from thread import *


class server:
        
    def __init__(self,HOST,PORT,Prev_PORT,Next_PORT):
        self.HOST=HOST
        self.PORT=PORT
        self.Next=-1
        self.Prev=-1
        self.client_socket_prev=socket.socket()
        self.client_socket_next=socket.socket()
        #open socket
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # bind socket at the given host,port
        try:
            self.s.bind((HOST, PORT))
        except socket.error as msg:
            print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
            sys.exit()
        #sockets maximum listeners. We suppose they cant be more than 5
        self.s.listen(5)
        
        self.connection_list=[self.s] #list of socket that send to server
        self.write_to_client=[] #list of socket that server want to write
        if Prev_PORT!=-1:
            self.client_socket_prev = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client_socket_prev.connect(('localhost', Prev_PORT))
            self.client_socket_prev.send('next ' +  str(self.PORT))
            self.client_socket_prev.recv(1024)
            self.Prev=Prev_PORT
        if Next_PORT!=-1:
            if Next_PORT!=Prev_PORT:
                self.client_socket_next = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.client_socket_next.connect(('localhost', Next_PORT))
            else:
                self.client_socket_next=self.client_socket_prev
            
            self.client_socket_next.send('prev ' + str(self.PORT))
            self.client_socket_next.recv(1024)
            self.Next=Next_PORT
            
        self.message_queues={} #servers' reply messages
        
    def accept_connection(self):
        while 1:
            read_sockets,write_sockets,error_sockets = select.select(self.connection_list,self.write_to_client,self.connection_list)
            for sock in read_sockets:
                if sock==self.s:
                    #wait to accept a connection - blocking call
                    conn, addr = self.s.accept()
                    self.connection_list.append(conn)
                    print 'Connected with ' + addr[0] + ':' + str(addr[1])
                    self.message_queues[conn] = Queue.Queue()
                
                else:
                    data = sock.recv(1024)
                    if data=='quit':
                        if sock in self.write_to_client:
                            self.write_to_client.remove(sock)
                        self.connection_list.remove(sock)
                        sock.close()
                        del self.message_queues[sock]
                    elif data:
                        if data[:4]=='next':
                            if self.Next!=-1:
                                self.client_socket_next.close()
                            self.Next = int(re.search(r'\d+', data).group())  
                            self.client_socket_next = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            self.client_socket_next.connect(('localhost', self.Next))
                            self.message_queues[sock].put('Connection granted...'+str(self.Next))
                        elif data[:4]=='prev':
                            if self.Prev !=-1:
                                self.client_socket_prev.close()
                            self.Prev = int(re.search(r'\d+', data).group())
                            self.client_socket_prev = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            self.client_socket_prev.connect(('localhost', self.Prev))
                            self.message_queues[sock].put('Connection granted...'+str(self.Prev))
                        else:
                            self.message_queues[sock].put('You send me... '+data)
                        if sock not in self.write_to_client:
                            self.write_to_client.append(sock)
                                            
            for sock in write_sockets:
                try:
                    next_msg = self.message_queues[sock].get_nowait()
                except Queue.Empty:
                    # No messages waiting so stop checking for writability.
                    self.write_to_client.remove(sock)
                else:
                    sock.send(next_msg)

            for sock in error_sockets:
                print >>sys.stderr, 'handling exceptional condition for', sock.getpeername()
                # Stop listening for input on the connection
                self.connection_list.remove(sock)
                if sock in self.write_to_client:
                    self.write_to_client.remove(sock)
                sock.close()
                del self.message_queues[sock]
        self.s.close()

    def get_port(self):
        return self.PORT

    def get_host(self):
        return self.HOST

    def update(self):
        return NotImplemented
    
