"""Handles all client side operations"""

class client(object):
    """Client class"""
    def init():
        pass

    def query(key):
        """Queries for a certain key and returns its value"""
        raise NotImplementedError

    def insert(key, value):
        """Inserts a (key, value) pair to the DHT"""
        raise NotImplementedError
