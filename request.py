"""Request class to model the different operations of the DHT"""

import pickle

class Request(object):
    """Models the requests and it is the object sent throught the sockets

    WARNING: Because of send and pickle this module
    must be imported as a whole and not as
        from request import Request
    but plainly
        import request"""
    def __init__(self, req_type, data=None):
        """Constructor"""
        if data == None:
            data = req_type.split(', ')
            if len(data) == 2:
                if data[0] == 'query':
                    self.type = 'query'
                    self.data = data[1]
                else:
                    self.type = 'insert'
                    self.data = data
            elif len(data) == 3:
                self.type = data[0]
                self.data = data[1:]
            else:
                raise ValueError('Bad Request')
        else:
            self.type = req_type
            self.data = data

    def send(self, sock):
        """Sends the request to the socket"""
        pickle.dump(self, sock)
