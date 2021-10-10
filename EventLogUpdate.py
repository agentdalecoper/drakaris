class EventLogUpdate:

    def __init__(self, json, data_class):
        self.event_id = json['id']
        self.op = json['op']
        self.ts = json['ts']
        self.set = data_class(json['set'])
