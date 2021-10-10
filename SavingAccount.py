class SavingAccount:
    """"data": {
    "savings_account_id": "sa1",
    "balance": 0,
    "interest_rate_percent": 1.5,
    "status": "ACTIVE"
  }"""

    def __init__(self, json):
        self.savings_account_id = json.get('savings_account_id')
        self.balance = json.get('balance')
        self.interest_rate_percent = json.get('interest_rate_percent')
        self.status = json.get('status')

    def to_dict(self):
        return {
            'savings_account_id': self.savings_account_id,
            'balance': self.balance,
            'interest_rate_percent': self.interest_rate_percent,
            'status': self.status,
        }
