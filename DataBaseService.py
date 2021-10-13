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
pd.set_option('display.precision', 10)
pd.options.mode.chained_assignment = None

def read_dataframe(path):
    result_lst = []
    for filename in os.listdir(path):
        js = json.load(open(f"{path}/{filename}"))
        event_log = EventLog(js)
        result_lst.append(event_log)

    result = pd.DataFrame.from_records([s.to_dict() for s in result_lst])
    result = result.sort_values('ts')
    # print(result)
    return result


def compute_dfs(df, entity_class):
    result_rows = []
    groups = df.groupby('id')
    for name, group in groups:
        for index, row in group.iterrows():
            if row['op'] == 'c':
                current_entity = entity_class(row['data'], row['ts'])
                # print(f"Created current object: {current_entity.to_dict()}")

                result_rows.append(current_entity.to_dict())
            else:
                temp_entity = entity_class(row['set'], row['ts'])
                members = [attr for attr in dir(temp_entity) if
                           not callable(getattr(temp_entity, attr)) and not attr.startswith("__")]
                for member in members:
                    if getattr(temp_entity, member) is not None:
                        setattr(current_entity, member, getattr(temp_entity, member))
                        # print(f"Updated current member {member}: {current_entity.to_dict()}")

                result_rows.append(current_entity.to_dict())

    result = pd.DataFrame.from_records(result_rows).sort_values('ts')
    print(result)
    return result


accounts_log_df = read_dataframe("data/accounts")
accounts_history_df = compute_dfs(accounts_log_df, Account)

cards_logs_df = read_dataframe("data/cards")
cards_history_df = compute_dfs(cards_logs_df, Card)

savings_accounts_logs_df = read_dataframe("data/savings_accounts")
savings_accounts_history_df = compute_dfs(savings_accounts_logs_df, SavingAccount)

lst = []
columns = accounts_history_df.columns
columns = columns.append(cards_history_df.columns)
columns = columns.append(savings_accounts_history_df.columns)
columns = list(dict.fromkeys(columns))
df = pandas.DataFrame(columns=columns)


def get_status_at_time(ts):
    # print(ts)
    acc_row = accounts_history_df.iloc[[accounts_history_df[accounts_history_df['ts'] <= ts]['ts'].idxmax()]]
    if acc_row['card_id'].values[0]:
        card_row = join_table(acc_row, ts, cards_history_df, 'card_id')
        acc_row = acc_row.merge(card_row, on=['card_id'], suffixes=('', '_card'))

    if acc_row['savings_account_id'].values[0]:
        saving_row = join_table(acc_row, ts, savings_accounts_history_df, 'savings_account')
        acc_row = acc_row.merge(saving_row, on=['savings_account_id'], suffixes=('', '_savings_account'))

    # print(acc_row)
    acc_row['ts_account'] = acc_row['ts']
    acc_row['ts'] = ts
    return acc_row


# 1. get card_id / saving_id from account row
# 2. find card/saving account by ts and id
# 3. if found == None, return none row
def join_table(acc_row, ts, df, join_id):
    id = acc_row[join_id].values[0]
    # print('ID')
    # print(id)
    if not id:
        return pandas.DataFrame(columns=df.columns, index=[0])
    # found_row = df[df[(df['ts'] <= ts) & (df[join_id] == id)]['ts'].idxmax()]

    found_row = df[(df['ts'] <= ts)]
    found_row = found_row[found_row[join_id] == id]

    found_row = found_row.loc[[found_row['ts'].idxmax()]]
    found_row = found_row.rename(columns={'status': 'status_' + join_id})
    # print(found_row)
    return found_row


ts = accounts_history_df['ts'].append(cards_history_df['ts']).append(
    savings_accounts_history_df['ts']).drop_duplicates()

lst = []
for row in ts:
    acc_row = get_status_at_time(row)
    lst.append(acc_row)

df = pd.DataFrame.append(df, lst).sort_values('ts')
df.drop(columns=['status'], inplace=True)

print(df)