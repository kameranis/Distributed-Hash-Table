"""Request class to model the different operations of the DHT"""



class Request(object):
    """Models the requests and it is the object sent throught the sockets"""
    def __init__(self, type, data=None):
        """Constructor"""
        if data == None:
            data = type.split(', ')
            if len(data) == 2:
                if data[0] == 'query':
                    self.type = 'query'
                    self.data = data[1]
                else:
                    self.type = 'insert'
                    self.data = data
            elif data == 3:
                self.type = data[0]
                self.data = data[1:]
            else:
                raise ValueError('Bad Request')
        else:
            self.type = type
            self.data = data