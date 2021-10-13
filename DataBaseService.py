import pandas
import pandas as pd
import os
import json
from Entity.Account import Account
from Entity.Events.EventLog import EventLog
from Entity.Card import Card
from Entity.SavingAccount import SavingAccount
import configparser

pd.set_option("max_columns", None)
pd.set_option('max_colwidth', None)
pd.set_option("expand_frame_repr", False)
pd.set_option('display.precision', 10)
pd.options.mode.chained_assignment = None


def process_tables(accounts_path, cards_path, savings_accounts_path):
    accounts_log_df = read_dataframe(accounts_path)
    accounts_history_df = get_history_table(accounts_log_df, Account)

    cards_logs_df = read_dataframe(cards_path)
    cards_history_df = get_history_table(cards_logs_df, Card)

    savings_accounts_logs_df = read_dataframe(savings_accounts_path)
    savings_accounts_history_df = get_history_table(savings_accounts_logs_df, SavingAccount)

    # create df with columns with all tables
    joined_df_columns = accounts_history_df.columns
    joined_df_columns = joined_df_columns.append(cards_history_df.columns)
    joined_df_columns = joined_df_columns.append(savings_accounts_history_df.columns)
    joined_df_columns = list(dict.fromkeys(joined_df_columns))
    joined_df = pandas.DataFrame(columns=joined_df_columns)

    # get dataframe with unique timestamps across all tables
    timestamps_df = accounts_history_df['ts'].append(cards_history_df['ts']).append(
        savings_accounts_history_df['ts']).drop_duplicates()

    # iterate over all timestamps and get current state of the system for each ts
    history_rows = []
    for timestamp in timestamps_df:
        acc_row = get_history_entry_at_time(timestamp, accounts_history_df, cards_history_df,
                                            savings_accounts_history_df)
        history_rows.append(acc_row)

    joined_df = pd.DataFrame.append(joined_df, history_rows).sort_values('ts')
    joined_df.drop(columns=['status'], inplace=True)
    print(joined_df)


def read_dataframe(path):
    events = []
    for filename in os.listdir(path):
        js = json.load(open(f"{path}/{filename}"))
        event_log_entry = EventLog(js)
        events.append(event_log_entry)

    result = pd.DataFrame.from_records([s.to_dict() for s in events])
    result = result.sort_values('ts')
    return result


def get_history_table(df, entity_class):
    result_rows = []
    groups = df.groupby('id')
    for name, group in groups:
        for index, row in group.iterrows():
            # if operation is create, then 'data' is the body
            # current entity should be always created before any of updates
            if row['op'] == 'c':
                current_entity = entity_class(row['data'], row['ts'])
                result_rows.append(current_entity.to_dict())
            else:
                temp_entity = entity_class(row['set'], row['ts'])
                # set dynamicly field of the current entity from update log entry
                members = [attr for attr in dir(temp_entity) if
                           not callable(getattr(temp_entity, attr)) and not attr.startswith("__")]
                for member in members:
                    if getattr(temp_entity, member) is not None:
                        setattr(current_entity, member, getattr(temp_entity, member))
                result_rows.append(current_entity.to_dict())

    result = pd.DataFrame.from_records(result_rows).sort_values('ts')
    print(result)
    return result


# for given time calculate actual state of tables
def get_history_entry_at_time(ts, accounts_history_df, cards_history_df, savings_accounts_history_df):
    # find account history row most recent to this timestamp
    account_row = accounts_history_df.iloc[[accounts_history_df[accounts_history_df['ts'] <= ts]['ts'].idxmax()]]

    # if join id exists
    if account_row['card_id'].values[0]:
        card_row = get_corresponding_event_entry(account_row, ts, cards_history_df, 'card_id')
        account_row = account_row.merge(card_row, on=['card_id'], suffixes=('', '_card'))

    if account_row['savings_account_id'].values[0]:
        saving_row = get_corresponding_event_entry(account_row, ts, savings_accounts_history_df, 'savings_account_id')
        account_row = account_row.merge(saving_row, on=['savings_account_id'], suffixes=('', '_savings_account'))

    # ts_account is the accounts timestamp
    account_row['ts_account'] = account_row['ts']
    account_row['ts'] = ts

    return account_row


# get corresponding entry in table for this timestamp ts
def get_corresponding_event_entry(account_row, ts, df, join_id):
    # get join id
    id = account_row[join_id].values[0]

    # find entry which was happend before ts and which is correspondand to this id
    found_row = df[(df['ts'] <= ts)]
    found_row = found_row[found_row[join_id] == id]

    # get most recent state
    found_row = found_row.loc[[found_row['ts'].idxmax()]]

    # rename status column to the concrete table name
    found_row = found_row.rename(columns={'status': 'status_' + join_id})
    return found_row


config = configparser.RawConfigParser()
config.read('config.properties')

process_tables(config.get("Events", "account.path"),
               config.get("Events", "card.path"),
               config.get("Events", "savings_account.path"))
