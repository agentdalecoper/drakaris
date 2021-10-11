class Account:
    """  "data": {
    "account_id": "a1",
    "name": "Anthony",
    "address": "New York",
    "phone_number": "12345678",
    "email": "anthony@somebank.com"
  }
    """

    def __init__(self, json, ts):
        self.account_id = json.get('account_id')
        self.name = json.get('name')
        self.address = json.get('address')
        self.phone_number = json.get('phone_number')
        self.email = json.get('email')
        self.card_id = json.get('card_id')
        self.savings_account_id = json.get('savings_account_id')
        self.ts = ts

    def to_dict(self):
        return {
            'account_id': self.account_id,
            'name': self.name,
            'address': self.address,
            'phone_number': self.phone_number,
            'email': self.email,
            'card_id': self.card_id,
            'savings_account_id': self.savings_account_id,
            'ts': self.ts
        }
