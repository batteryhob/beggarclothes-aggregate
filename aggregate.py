import boto3
from boto3.dynamodb.conditions import Key
import configparser
import pymysql

from pytz import timezone
from datetime import datetime
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
            ( `designer`, `date`, `cnt`, `view`, `regdate`)
            VALUES
            ( %(designer)s, %(date)s, %(cnt)s, %(view)s, NOW());
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
            `tag`, `name`, `designer_seq` 
            FROM tbl_designer_tags A
            LEFT JOIN tbl_designer B
            ON A.designer_seq = B.seq
            WHERE `use` = true
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
        pdate = now.strftime("%Y.%m.%d.")

        rows = query_designers(pdesigner, pdate)
        cnt = 0
        view = 0
        for row in rows:
            cnt = cnt + 1
            view = view + int(row['view'].replace(',',''))

        #aggregate insert
        write_to_rds(
            designer=pdesigner,
            date=pdate,
            cnt=cnt,
            view=view
        )
            