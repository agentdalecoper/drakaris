"""{
  "data": {
    "card_id": "c1",
    "card_number": "11112222",
    "credit_used": 0,
    "monthly_limit": 30000,
    "status": "PENDING"
  }
}"""


class Card:

    def __init__(self, json, ts):
        self.card_id = json.get('card_id')
        self.card_number = json.get('card_number')
        self.credit_used = json.get('credit_used')
        self.monthly_limit = json.get('monthly_limit')
        self.status = json.get('status')
        self.ts = ts

    def to_dict(self):
        return {
            'card_id': self.card_id,
            'card_number': self.card_number,
            'credit_used': self.credit_used,
            'monthly_limit': self.monthly_limit,
            'status': self.status,
            'ts': self.ts
        }
