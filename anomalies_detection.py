import pandas as pd
import numpy as np
import pandahouse as ph
import matplotlib.pyplot as plt
import seaborn as sns

import telegram
import io
import datetime as dt

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import warnings
warnings.filterwarnings("ignore")

custom_params = {"axes.spines.right": False, "axes.spines.top": False, 'figure.figsize': (10, 4)}
sns.set_theme(style="ticks", rc=custom_params, font_scale = 0.85)

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20221020'
}

default_args = {
    'owner': 'a-jurmin-11',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': dt.timedelta(minutes = 5),
    'start_date': dt.datetime(2022, 11, 21),
}

schedule_interval = '*/5 * * * *'

bot = telegram.Bot(token = '5918664814:AAF3OhSVOqZPWtJWZC9hwX87NISIbhpoU6o')
chat_id = 296672696

def report_maker(df, variable_name):
    
    df = df[df['time'].dt.date >= dt.date.today()]
    df.loc[:, 'time'] = df.loc[:, 'time'].dt.strftime('%H:%M')

    if df.metric.iloc[-1] > df.up.iloc[-1] or True:
        relation = 'высокое'
        diff = df.metric.iloc[-1] / df.up.iloc[-1]
        value = int(df.up.iloc[-1])
    elif df.metric.iloc[-1] < df.low.iloc[-1]:
        relation = 'низкое'
        diff = df.metric.iloc[-1] / df.low.iloc[-1]
        value = int(df.low.iloc[-1])

    message = f'''Метрика {variable_name.name.lower().replace('_', ' ')} имеет аномально {relation} значение:\n* Текущее значение {df.metric.iloc[-1]}.\n* Отклонение {diff:.2%} ({value}).\n* Больше деталей тут: http://superset.lab.karpov.courses/r/2484'''
    bot.sendMessage(chat_id = chat_id, text = message)

    fig = plt.figure()
    ax = sns.lineplot(x=df.time, y=df.up, linewidth=1.5, dashes = False, ci = False)
    ax = sns.lineplot(x=df.time, y=df.low, linewidth=1.5, dashes = False, ci = False)
    ax = sns.lineplot(x=df.time, y=df.metric, linewidth=3.5, dashes = False, ci = False)
    ax.set_xticks(df.time[1::4])
    plt.xticks(rotation = 30)
    ax.set_title(variable_name.name.capitalize().replace('_', ' '), fontsize = 14)
    ax.set_xlabel(' ')
    ax.set_ylabel(' ')
    ax.lines[0].set_linestyle("--")
    ax.lines[1].set_linestyle("--")

    plot_object = io.BytesIO()
    fig.savefig(plot_object)
    plot_object.seek(0)
    bot.sendPhoto(chat_id = chat_id, photo = plot_object)


@dag(default_args = default_args, schedule_interval = schedule_interval, catchup = False)
def anomalies_detector():
    
    @task()
    def get_feed_df():

        query_feed = '''
        select
            toStartOfFifteenMinutes(time) as fm_time,
            uniqExact(user_id) as active_users_in_feed,
            countIf(action, action = 'view') as views,
            countIf(action, action = 'like') as likes,
            round(countIf(action, action = 'like') / countIf(action, action = 'view') * 100, 2) as ctr
        from simulator_20221020.feed_actions
        where fm_time between today() - 1 and now() - interval 15 minute
        group by fm_time
        order by fm_time asc
        '''

        feed = ph.read_clickhouse(query_feed, connection = connection)

        return feed
    
    @task()
    def get_messanger_df():

        query_messanger = '''
        select
            toStartOfFifteenMinutes(time) as fm_time,
            uniqExact(user_id) as active_users_in_messenger,
            count(reciever_id) as number_of_sent_messages
        from simulator_20221020.message_actions
        where fm_time between today() - 1 and now() - interval 15 minute
        group by fm_time
        order by fm_time asc
        '''

        messages = ph.read_clickhouse(query_messanger, connection = connection)

        return messages

    
    @task()
    def sigma_outlier_checker(variable_name, time, window = 4, z = 2.58):

        df = pd.DataFrame({'metric': [], 'up': [], 'low': []})

        roll_mean = variable_name.shift(1).rolling(window = window, min_periods = 1).mean()
        difference = variable_name - roll_mean
        df['time'] = time
        df['metric'] = variable_name
        df['up'] = roll_mean + z * np.std(difference)
        df['low'] = roll_mean - z * np.std(difference)

        if (variable_name.iloc[-1] > df['up'].iloc[-1]) or (variable_name.iloc[-1] < df['low'].iloc[-1]) or True:
            report_maker(df, variable_name)

    feed = get_feed_df()
    messages = get_messanger_df()

    sigma_outlier_checker(messages['active_users_in_messenger'], messages['fm_time'])
    sigma_outlier_checker(messages['number_of_sent_messages'], messages['fm_time'])
    sigma_outlier_checker(feed['active_users_in_feed'], feed['fm_time'])
    sigma_outlier_checker(feed['views'], feed['fm_time'])
    sigma_outlier_checker(feed['likes'], feed['fm_time'])
    sigma_outlier_checker(feed['ctr'], feed['fm_time'])
    

anomalies_detector_an = anomalies_detector()

