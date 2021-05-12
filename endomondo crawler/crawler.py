import requests
import re
import json
from fake_useragent import UserAgent
import pandas as pd
import time
import pymysql


def get_workout(user_id, workout_id):
    url = 'https://www.endomondo.com/rest/v1/users/' + \
        str(user_id)+'/workouts/'+str(workout_id)
    ua = UserAgent()
    ua.chrome
    headers = {"User-Agent": ua.random}
    # headers = {'User-Agent': 'Chrome/76.0.1231.132'}
    res = requests.get(url, headers=headers)
    key = ['calories', 'distance']
    temp = {}
    print(user_id, workout_id)
    if res.status_code == 200:
        j = json.loads(res.text)
        temp = {}
        temp.update({k: j[k] for k in key})
        return temp
    else:
        print('Return Error')
        temp = {}
        temp.update({k: -1 for k in key})
        return temp


def insert_db(calories, distance, work_id):
    db = pymysql.connect(host="106.52.6.175",
                         user="bonan",
                         password="123456",
                         port=3306,  # mysql
                         database="endomondo",
                         charset='utf8')
    print('DB')
    cursor = db.cursor()
    sql = 'update workout set calories=%s,distance=%s where id=%s;'
    try:
        cursor.execute(sql, [calories, distance, work_id])
        db.commit()
    except:
        record('./records/failed.csv', row['id'])
        print('update failed')
        db.rollback()
    cursor.close()
    db.close()


def record(path, workout_id):
    df = pd.read_csv(path)
    df = df.append({'id': workout_id}, ignore_index=True)
    df.to_csv(path, index=0)


def save_csv(filename, res, index):
    df = pd.read_excel(filename, sheet_name='workout')
    df.at[index, 'calories'] = res['calories']
    df.at[index, 'distance'] = res['distance']
    df.to_excel(filename, sheet_name='workout', index=0)


if __name__ == "__main__":
    # 需要按照自己的batch进行修改
    is_save_local = True
    batch = 2
    batch_name = './fitness_batch{}.xlsx'.format(batch)
    batch_result = './results/batch{}_result.xlsx'.format(batch)
    
    df = pd.read_excel(batch_name, sheet_name='workout')
    for index, row in df.iterrows():
        success = list(pd.read_csv('./records/success.csv')['id'])
        failed = list(pd.read_csv('./records/failed.csv')['id'])
        if row['id'] in success+failed:
            # print('{} existed...'.format(row['id']))
            continue
        try:
            res = get_workout(row['user_id'], row['id'])
            if is_save_local:
                save_csv(batch_result, res, index)
            else:
                insert_db(res['calories'], round(
                    res['distance'], 2), row['id'])
            if res['calories'] == -1:
                record('./records/failed.csv', row['id'])
            else:
                record('./records/success.csv', row['id'])
            print('inserting...'+str(index)+':'+str(res))
        except:
            print('Something wrong at {}'.format(index))
            insert_db(-1, -1, row['id'])
            record('./records/failed.csv', row['id'])
            print('inserting...unnormal'+str(index)+':error')
