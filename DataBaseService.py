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
                current_entity = entity_class(row['data'], row['ts'])
                print(f"Created current object: {current_entity.to_dict()}")

                result_rows.append(current_entity.to_dict())
            else:
                temp_entity = entity_class(row['set'], row['ts'])
                members = [attr for attr in dir(temp_entity) if
                           not callable(getattr(temp_entity, attr)) and not attr.startswith("__")]
                for member in members:
                    if getattr(temp_entity, member) is not None:
                        setattr(current_entity, member, getattr(temp_entity, member))
                        print(f"Updated current member {member}: {current_entity.to_dict()}")

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


def join_hist_table(accounts_df, join_df, join_id, suffix):
    res = accounts_df.merge(join_df, on=[join_id], how='left', suffixes=('', suffix))
    cols = accounts_df.columns.tolist()
    res = res.groupby(cols, dropna=False)
    resul = []
    for name, group in res:
        not_n = group[group['ts'] >= group[f'ts{suffix}']].sort_values(f'ts{suffix}')
        print("----" + f"sufix {suffix}")
        print(cols)
        print(name)
        print(not_n)

        if len(not_n) > 0:
            r = group.loc[not_n[f'ts{suffix}'].idxmax()]
            #print(name)
            #print(r)
            resul.append(r)
        else:
            #print(name)
            #print(group.iloc[0])
            resul.append(group.iloc[0])
    joined_df = pd.DataFrame.from_records(resul).sort_values('ts')
    print(joined_df)
    return joined_df


joined = join_hist_table(accounts_history_df, cards_history_df, 'card_id', '_card')
result_join = join_hist_table(joined, savings_accounts_history_df, 'savings_account_id', '_acc')