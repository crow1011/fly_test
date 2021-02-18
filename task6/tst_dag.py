import os
import datetime as dt
import sqlite3
import requests
import pandas as pd
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2020, 2, 11),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}


# task1

def download_titanic_dataset():
    '''
    Запрашивает построчно dataset titanic и записывает в FILENAME
    Стадия: Extract
    '''
    FILENAME = os.path.join(os.path.expanduser('~/data_airflow'), 'titanic.csv')
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(FILENAME, 'w', encoding='utf-8') as f:
        for chunk in response.iter_lines():
            f.write('{}\n'.format(chunk.decode('utf-8')))


# task2
def only_survived():
    '''
    Обрабатывает данные из файла FILENAME_SOURCE
    Оставляет только выживших и сохраняет в файл FILENAME_SURVIVED
    Стадия: Transform
    '''
    FILENAME_SOURCE = os.path.join(os.path.expanduser('~/data_airflow'), 'titanic.csv')
    df_tmp = pd.read_csv(FILENAME_SOURCE)
    df = df_tmp[df_tmp['Survived']==1]
    df['Name'] = df['Name'].str.replace('(', '[')
    df['Name'] = df['Name'].str.replace(')', ']')
    df.index = range(len(df.index))
    FILENAME_SURVIVED = os.path.join(os.path.expanduser('~/data_airflow'), 'titanic_survived.csv')
    df.to_csv(FILENAME_SURVIVED, index=False)


# task3
def to_db():
    '''
    Читает готовые данные из файла FILENAME_SURVIVED
    Создает(если существует пересоздает) таблицу в sqlite3 и записывает файл в базу
    Стадия: Load
    '''
    FILENAME_SURVIVED = os.path.join(os.path.expanduser('~/data_airflow'), 'titanic_survived.csv')
    DBNAME = os.path.join(os.path.expanduser('~/data_airflow'), 'titanic.db')
    con = sqlite3.connect(DBNAME)
    cur = con.cursor()
    cur.execute('DROP TABLE IF EXISTS titanic')
    con.commit()
    df = pd.read_csv(FILENAME_SURVIVED)
    sql_create_table = ''' CREATE TABLE IF NOT EXISTS titanic(
                            Survived integer,
                            Pclass interger,
                            Name varchar(200),
                            Sex varchar(30),
                            Age integer,
                            Siblings_Spouses integer,
                            Parents_Children integer,
                            Fare real
                            ) '''
    cur.execute(sql_create_table)
    con.commit()
    for _, val in df.iterrows():
        sql_add_line = f''' INSERT INTO titanic VALUES(
                            {val['Survived']},
                            {val['Pclass']},
                            "{val['Name']}",
                            "{val['Sex']}",
                            {val['Age']},
                            {val['Siblings/Spouses Aboard']},
                            {val['Parents/Children Aboard']},
                            {val['Fare']}
                            )  '''
        cur.execute(sql_add_line)
    con.commit()


with DAG(dag_id='titanic_to_db', default_args=args, schedule_interval=None) as dag:
    create_titanic_dataset = PythonOperator(
        task_id='download_titanic_dataset',
        python_callable=download_titanic_dataset,
        dag=dag
    )
    only_survived = PythonOperator(
        task_id='only_survived',
        python_callable=only_survived,
        dag=dag
    )
    pivot_titanic_dataset = PythonOperator(
        task_id='to_db',
        python_callable=to_db,
        dag=dag
    )
    create_titanic_dataset >> only_survived >> pivot_titanic_dataset
