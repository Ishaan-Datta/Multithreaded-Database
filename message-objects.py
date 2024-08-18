import json
from datetime import datetime

class QueryMessage:
    def __init__(self, query, query_type):
        self.query = query
        self.query_type = query_type
        self.date_added = datetime.now().isoformat()

    def to_json(self):
        return json.dumps(self.__dict__)

class TransactionMessage:
    def __init__(self, operation, data):
        self.operation = operation
        self.data = data
        self.date_added = datetime.now().isoformat()

    def to_json(self):
        return json.dumps(self.__dict__)
