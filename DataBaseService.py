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
res = savings_accounts_history_df.merge(accounts_history_df, on=['savings_account_id'], how='left', suffixes=('_savings_account', '_account'))
#res = res.merge(cards_history_df, on=['card_id'], how='outer', suffixes=('', '_card_id'))
res = res.sort_values('ts_savings_account')
# далее группируем по [account_id, savings_account_id, ts_acc] и находим максимальный ts в группе но который меньше чем
# (если такой записи нет, то по идее вставляет NAN, но в этом плане пофиг поскольку если есть ссылка то должна быть эта
print('======')
#print(res)
# стоой погоди - а если не мерджить - а просто историю предлагать - тоесть забить на accounts
# заить на мердж таймстампов чтоли хочу
# давай короче вот что - сделаю приджоин обеих таблиц чтобы работало потом посмотрю



grouped = res.groupby(['account_id', 'savings_account_id', 'ts_savings_account'], dropna=False)
lst=[]
for cols, group in grouped:
    if len(group) == 1:
        lst.append(group.iloc[0])
        continue

    closest_row = group[group['ts_account'] <= group[f'ts_savings_account']].sort_values(f'ts_savings_account').iloc[0]
    lst.append(closest_row)

print(pd.DataFrame.from_records(lst))


res = cards_history_df.merge(accounts_history_df, on=['card_id'], how='left', suffixes=('_card', '_account'))
#res = res.merge(cards_history_df, on=['card_id'], how='outer', suffixes=('', '_card_id'))
res = res.sort_values('ts_savings_account')
# далее группируем по [account_id, savings_account_id, ts_acc] и находим максимальный ts в группе но который меньше чем
# (если такой записи нет, то по идее вставляет NAN, но в этом плане пофиг поскольку если есть ссылка то должна быть эта
print('======')
#print(res)
# стоой погоди - а если не мерджить - а просто историю предлагать - тоесть забить на accounts
# заить на мердж таймстампов чтоли хочу
# давай короче вот что - сделаю приджоин обеих таблиц чтобы работало потом посмотрю


# тааак а что если все в один union, далее показывать по каждому таймстампу - короче таймстамп должен быть только один в каждый момент времени
# если один и тот же таймстамп в одно время то он попадет в группу [account_id, card_id, ts]. Если попал то мерджим
# короче - мерджим все в одну таблицу и один в итоге один ts на все
#

grouped = res.groupby(['account_id', 'savings_account_id', 'ts_savings_account'], dropna=False)
lst=[]
for cols, group in grouped:
    if len(group) == 1:
        lst.append(group.iloc[0])
        continue

    closest_row = group[group['ts_account'] <= group[f'ts_savings_account']].sort_values(f'ts_savings_account', ascending=False).iloc[0]
    lst.append(closest_row)

print(pd.DataFrame.from_records(lst))


# а вот смотри - смерджить все в одно и потом идти и брать из существующих таблиц джойном