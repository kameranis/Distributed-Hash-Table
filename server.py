"""Server side code for the DHT

Contains the code executed for joining, inserting, quering and departing
of the nodes."""

class server(object):
    """Main server object"""

    def __init__():
        raise NotImplementedError

    def join(sock):
        """Performs all necessary actions for a node to join the DHT"""
        raise NotImplementedError

    def depart():
        """Handles the graceful departure of the server"""
        raise NotImplementedError

    def insert(key, value):
        """Inserts the (key, value) pair"""
        raise NotImplementedError

    def query(key):
        """Returns the value of the associated key"""
        raise NotImplementedError

    def key_in_range(key):
        """Returns True if the key belongs to this node"""
        return this.previous_key < key <= this.key

    def process_request(request):
        request_type, request_payload = request
        if request_type == 'query':
            if key_in_range(request_payload[0]):
                return query(request_payload[0])
            else:
                forward(request)
        else if request_type == 'insert':
            if key_in_range(request_payload[0]):
                return insert(request_payload[0], request_payload[1])
            else:
                forward(request)
