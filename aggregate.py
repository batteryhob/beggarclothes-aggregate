import boto3
from boto3.dynamodb.conditions import Key
import configparser
import pymysql

from pytz import timezone
from datetime import datetime, timedelta
import time


# Nas Insert
def write_to_rds(**kwargs):
    config = configparser.ConfigParser()
    config.read('config.ini')

    con = pymysql.connect(host=config['Nas']['HOST'], port=3307, user=config['Nas']['USER'], passwd=config['Nas']['PASSWORD'], db=config['Nas']['NAME'], charset='utf8')
    cur = con.cursor()
    cur.execute(
        query='''
            INSERT INTO `tbl_designer_agg`
            ( `designer`, `community_seq`, `date`, `cnt`, `view`, `regdate`)
            VALUES
            ( %(designer)s, %(community_seq)s, %(date)s, %(cnt)s, %(view)s, NOW());
        ''',
        args=kwargs)
    cur.close()
    con.commit()
    con.close()


def get_designers():
    config = configparser.ConfigParser()
    config.read('/app/config.ini', encoding='utf-8')

    con = pymysql.connect(host=config['Nas']['HOST'], port=3307, user=config['Nas']['USER'], passwd=config['Nas']['PASSWORD'], db=config['Nas']['NAME'], charset='utf8')
    cur = con.cursor()
    cur.execute(
        query='''
            SELECT 
            `seq`, `name`, `korean`
            FROM tbl_designer
        ''')
    rows = cur.fetchall()
    con.close()
    return rows


def query_designers(pdesigner, pdate, dynamodb=None):
    if not dynamodb:
        # 설정정보 불러오기
        config = configparser.ConfigParser()
        config.read('/app/config.ini', encoding='utf-8')
        session = boto3.Session(
                                    aws_access_key_id=config['AWS']['AccessKey'],
                                    aws_secret_access_key=config['AWS']['SecretKey'],
                                    region_name='ap-northeast-2'
                                )
        dynamodb = session.resource('dynamodb')


    table = dynamodb.Table('tbl_designer_gather')

    while True:
        if not table.global_secondary_indexes or table.global_secondary_indexes[0]['IndexStatus'] != 'ACTIVE':
            print('Waiting for index to backfill...')
            time.sleep(5)
            table.reload()
        else:
            break

    
    resp = table.query (
        # 쿼리에 사용할 인덱스 이름을 추가합니다.
        IndexName = "designer-date-index",
        KeyConditionExpression = Key('designer').eq(pdesigner) & Key('date').eq(pdate)
    )
    return resp['Items']


if __name__ == '__main__':

    targetDesigners = get_designers()
    for designer in targetDesigners:

        pdesigner = designer[1]
        now = datetime.now(timezone("Asia/Seoul"))
        day = timedelta(1)

        new_date = now - day
        pdate = new_date.strftime("%Y.%m.%d.")  

        # pdate = '2020.09.08.'
        print(pdate)

        rows = query_designers(pdesigner, pdate)
        cnt = 0
        view = 0
        tempDict = {}
        for row in rows:            
            if '만' in row['view']:
                viewcnt = float(row['view'].replace('만','')) * 10000
            else:
                viewcnt = int(row['view'].replace(',',''))

            if  len(tempDict) == 0:
                tempDict[int(row['community_seq'])] = [1, viewcnt]                
            else:
                if not int(row['community_seq']) in tempDict:
                    tempDict[int(row['community_seq'])] = [1, viewcnt]
                else:
                    tempDict[int(row['community_seq'])][0] = int(tempDict[int(row['community_seq'])][0]) + 1 # cnt
                    tempDict[int(row['community_seq'])][1] = int(tempDict[int(row['community_seq'])][1]) + viewcnt # view


        # 다했으면
        print("pdesigner:", pdesigner)
        for key in tempDict.keys():            
            print("community_seq:", int(key))
            print("cnt:", int(tempDict[key][0]))
            print("view:", int(tempDict[key][1]))
            #aggregate insert
            write_to_rds(
                designer=pdesigner,
                community_seq=int(key),
                date=pdate,
                cnt=int(tempDict[key][0]),
                view=int(tempDict[key][1])
            )
            