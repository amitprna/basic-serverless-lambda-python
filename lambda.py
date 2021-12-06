import json
import psycopg2
import pandas as pd
import requests
import io


def lambda_handler(event, context):
   
    lines = []
    with open('input') as f:
        lines = f.readlines()
    
    table = lines[0]
    url = f'https://raw.githubusercontent.com/amitprna/redshift_demo/main/{table}'
    df = pd.read_csv(url, error_bad_lines=False,sep = '|')
    print(df)
    
    conn = psycopg2.connect(dbname = 'dev',
                            host = 'redshift-cluster-1.cl2hqkv2gv8y.ap-south-1.redshift.amazonaws.com',
                            port = '5439',
                            user = 'awsuser',
                            password = 'Kindle,234')
    cur = conn.cursor()
    for q in range(len(df)):
        query = df.iloc[q][1]
        print(query)
        cur.execute(query)
        output = cur.fetchall()

        result = 'Pass'  if output == df.iloc[q][2] else 'Fail'
        print(output)
        query_data = f"insert into result(test_case_desc,test_query,expected_result,actual_result,result) values('{df.iloc[q][0]}','{df.iloc[q][1]}',{df.iloc[q][2]},{output[0][0]},'{result}');"
        query2 = query_data
        print(query2)
        cur.execute(query2)
    
    conn.commit()
    cur.close()
    conn.close()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
