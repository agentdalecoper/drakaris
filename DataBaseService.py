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
    #print(result)
    return result


def compute_dfs(df, entity_class):
    result_rows = []
    groups = df.groupby('id')
    for name, group in groups:
        for index, row in group.iterrows():
            if row['op'] == 'c':
                current_entity = entity_class(row['data'], row['ts'])
                #print(f"Created current object: {current_entity.to_dict()}")

                result_rows.append(current_entity.to_dict())
            else:
                temp_entity = entity_class(row['set'], row['ts'])
                members = [attr for attr in dir(temp_entity) if
                           not callable(getattr(temp_entity, attr)) and not attr.startswith("__")]
                for member in members:
                    if getattr(temp_entity, member) is not None:
                        setattr(current_entity, member, getattr(temp_entity, member))
                        #print(f"Updated current member {member}: {current_entity.to_dict()}")

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

# а что если смерджить все три в одну, и выделять группы по
# можно добавить тогда сразу один ивент туда в кардс для проверки

#res = accounts_history_df.merge(cards_history_df, on=['card_id'], how='outer', suffixes=('', '_card'))
res = savings_accounts_history_df.merge(accounts_history_df, on=['savings_account_id'], how='left', suffixes=('', '_savings_account'))
#res = res.merge(cards_history_df, on=['card_id'], how='outer', suffixes=('', '_card_id'))
res = res.sort_values('ts_savings_account')
# далее группируем по [account_id, savings_account_id, ts_acc] и находим максимальный ts в группе но который меньше чем
# (если такой записи нет, то по идее вставляет NAN, но в этом плане пофиг поскольку если есть ссылка то должна быть эта
print('======')
print(res)

grouped = res.groupby(['account_id', 'savings_account_id', 'ts_savings_account'], dropna=False)
lst=[]
for cols, group in grouped:
    if len(group) == 1:
        lst.append(group.iloc[0])
        continue

    closest_row = group[group['ts_savings_account'] >= group[f'ts_savings_account']].sort_values(f'ts_savings_account').iloc[0]
    lst.append(closest_row)

print(pandas.DataFrame.from_records(lst))

def join_hist_table(accounts_df, join_df, join_id, suffix):
    res = accounts_df.merge(join_df, on=[join_id], how='left', suffixes=('', suffix))
    #print(res)
    cols = accounts_df.columns.tolist()
    res = res.groupby(cols, dropna=False)
    resul = []
    for name, group in res:
        not_n = group[group['ts'] >= group[f'ts{suffix}']].sort_values(f'ts{suffix}')
        #print("----" + f"sufix {suffix}")
        #print(cols)
        #print(name)
        #print(not_n)

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
    #print(joined_df)
    return joined_df


joined = join_hist_table(cards_history_df, accounts_history_df, 'card_id', '_card')
result_join = join_hist_table(savings_accounts_history_df, accounts_history_df, 'savings_account_id', '_acc')


# а что если идти row by row и просто находить каждый раз нужный row для мерджа
# но тут сложности с тем, что три разные таблицы и типа у них разные истори


# так а может заджойнить к cardTable аккаунты, и к savingAccount table аккаунты.
# а может забить на те, где есть na и заджойнить только это. А na потом union сделать
# я бы сказал еще что уникальный id - это [account_id, ts], [card_id, ts_card], [account_saving_id, ts_account]
# если мы джойним к cardTable -> accountTable
# card_id ts_card account_id ts_account
# далее мы джойним к account_savingTable -> accountTable
# account_saving_id ts_account_sabing account_id ts_account