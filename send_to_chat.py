import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
from datetime import datetime, timedelta
import pandahouse

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {'host': 'http://clickhouse.{name}',
                      'database':'{db}',
                      'user':'{name}', 
                      'password':'{password}'
                     }
q = """
SELECT toDate(time) AS event_date,
            countIf(action = 'like') AS likes,
            countIf(action = 'view') AS view,
            likes/view AS ctr,
            uniqExact(user_id) AS dau
            FROM {db}.feed_actions
            GROUP BY toDate(time)
            HAVING toDate(time)>= today()-7 AND toDate(time) < today()
            """
#df = pandahouse.read_clickhouse(q, connection=connection)

my_token = '{token}' #токен бота
bot = telegram.Bot(token=my_token) # получаем доступ

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 't.ermoshina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 12),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_termoshina_telegram():
        
    @task
    def extract():
        df = pandahouse.read_clickhouse(q, connection=connection)
        return df
    
    @task
    def load(df):
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        dau = df[df['event_date'] == yesterday]['dau'].values[0]
        view = df[df['event_date'] == yesterday]['view'].values[0]
        likes = df[df['event_date'] == yesterday]['likes'].values[0]
        ctr = df[df['event_date'] == yesterday]['ctr'].values[0]
        
        # messsage
        chat_id = {chat_id}
        msg = f"""Отчет за {yesterday}:
                    DAU - {dau}
                    Просмотры - {view}
                    Лайки - {likes} 
                    CTR - {round(ctr,2)}
                """
        bot.sendMessage(chat_id=chat_id, text=msg)
        
        
        #DAU
        sns.lineplot(x = df['event_date'].dt.strftime('%b-%d'), y = df['dau'])
        plt.title('DAU за последние 7 дней')
        plt.xlabel('data')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'DAU.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        # Просмотры
        sns.lineplot(x = df['event_date'].dt.strftime('%b-%d'), y = df['view'])
        plt.title('Просмотры за последние 7 дней')
        plt.xlabel('data')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'view.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        # Лайки
        sns.lineplot(x = df['event_date'].dt.strftime('%b-%d'), y = df['likes'])
        plt.title('Лайки за последние 7 дней')
        plt.xlabel('data')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'likes.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        # CTR
        sns.lineplot(x = df['event_date'].dt.strftime('%b-%d'), y = df['ctr'])
        plt.title('CTR за последние 7 дней')
        plt.xlabel('data')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'ctr.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    df_final = extract()
    
    load(df_final)
    
t_ermoshina_telegram_dag = dag_termoshina_telegram()
