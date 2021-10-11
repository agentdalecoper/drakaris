class EventLogUpdate:

    def __init__(self, json, data_class):
        self.event_id = json['id']
        self.op = json['op']
        self.ts = json['ts']
        self.set = data_class(json['set'])



for i, row in accounts_history_df.iterrows():
    ts = row['ts']

    card_id = row["card_id"]
    card_row = cards_history_df.loc[(cards_history_df['ts'] <= ts) & (card_id == cards_history_df['card_id'])]

    if(len(card_row) == 0):
        res = None
    else:
        res = card_row.iloc[0]

    print(res)
