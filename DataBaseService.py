import pandas
import pandas as pd
import os
import json
from Account import Account
from EventLog import EventLog
from Card import Card
from SavingAccount import SavingAccount

pd.set_option("max_columns", None)
pd.set_option('max_colwidth', None)
pd.set_option("expand_frame_repr", False)


def read_dataframe(path):
    result_lst = []
    for filename in os.listdir(path):
        js = json.load(open(f"{path}/{filename}"))
        event_log = EventLog(js)
        result_lst.append(event_log)

    result = pd.DataFrame.from_records([s.to_dict() for s in result_lst])
    result = result.sort_values('ts')
    print(result)
    return result

def compute_dfs(df, entity_class):
    result_rows = []
    groups = df.groupby('id')
    for name, group in groups:
        for index, row in group.iterrows():
            if row['op'] == 'c':
                current_entity = entity_class(row['data'])
                print(f"Created current object: {current_entity.to_dict()}")
                result_rows.append(current_entity)
            else:
                temp_entity = entity_class(row['set'])
                members = [attr for attr in dir(temp_entity) if
                           not callable(getattr(temp_entity, attr)) and not attr.startswith("__")]
                result_rows.append(temp_entity)
                for member in members:
                    if getattr(temp_entity, member) is not None:
                        setattr(current_entity, member, getattr(temp_entity, member))
                        print(f"Updated current member {member}: {current_entity.to_dict()}")
                result_rows.append(current_entity)

    result = pd.DataFrame.from_records([s.to_dict() for s in result_rows])
    print(result)
    return result


accounts_log_df = read_dataframe("data/accounts")
accounts_history_df = compute_dfs(accounts_log_df, Account)

cards_logs_df = read_dataframe("data/cards")
cards_history_df = compute_dfs(cards_logs_df, Card)

savings_accounts_logs_df = read_dataframe("data/savings_accounts")
savings_accounts_history_df = compute_dfs(savings_accounts_logs_df, SavingAccount)
