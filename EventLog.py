class EventLog:
    """{
  "id": "a1globalid",
  "op": "c",
  "ts": 1577863800000,
  "data": {
    "account_id": "a1",
    "name": "Anthony",
    "address": "New York",
    "phone_number": "12345678",
    "email": "anthony@somebank.com"
  }
}"""

    def __init__(self, json):
        self.event_id = json['id']
        self.op = json['op']
        self.ts = json['ts']

        if json['op'] == 'c':
            self.data = json['data']
            self.set = None
        elif json['op'] == 'u':
            self.data = None
            self.set = json['set']

    def to_dict(self):
        return {
            'id': self.event_id,
            'op': self.op,
            'ts': self.ts,
            'data': self.data,
            'set': self.set
        }
