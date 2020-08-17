import boto3
from boto3.dynamodb.conditions import Key
import configparser
import time

import pymysql

# Nas Insert
def write_to_rds(**kwargs):
    config = configparser.ConfigParser()
    config.read('config.ini')

    con = pymysql.connect(host=config['Nas']['HOST'], port=3307, user=config['Nas']['USER'], passwd=config['Nas']['PASSWORD'], db=config['Nas']['NAME'], charset='utf8')
    cur = con.cursor()
    cur.execute(
        query='''
            INSERT INTO `tbl_saleinfo`
            ( `from`, `subject`, `content`, `saleflag`, `view`, `like`, `regdate`)
            VALUES
            ( %(fromstore)s, %(subject)s, %(content)s, %(sale)s, 0, 0, NOW());
        ''',
        args=kwargs)
    cur.close()
    con.commit()
    con.close()


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
    pdesigner = 'ADER error'
    pdate = '2020.01.07.'
    rows = query_designers(pdesigner, pdate)
    for row in rows:
        print(row)