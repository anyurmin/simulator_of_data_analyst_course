#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import telegram
import pandahouse as ph
import datetime as dt
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import io
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

sns.set(font_scale = 0.85,
        style = 'white',
        rc = {'figure.figsize': (16, 12)})

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
    'start_date': dt.datetime(2022, 11, 18),
}

schedule_interval = '0 11 * * *'

bot = telegram.Bot(token = '5726507498:AAEUaLuFT83of8_EnOfpFUaDLL4DqSpJCAs')
# chat_id = 296672696
chat_id = -817148946

@dag(default_args = default_args, schedule_interval = schedule_interval, catchup = False)
def report_for_feed():

    @task()
    def extract_data():
        
        query = '''
        select
            toDate(time) as date_time,
            uniq(user_id) as dau,
            countIf(action, action = 'view') as views,
            countIf(action, action = 'like') as likes,
            likes / views as ctr
        from simulator_20221020.feed_actions
        where date_time between today() - 7 and today() - 1
        group by date_time
        order by date_time desc
        '''

        df = ph.read_clickhouse(query, connection=connection)
        return df
    
    @task()
    def transformation(df):

        last_day_likes = df.iloc[0, 3]
        penultimate_day_likes = df.iloc[1, 3]
        last_day_views = df.iloc[0, 2]
        penultimate_day_views = df.iloc[1, 2]
        ctr_yesterday = df.iloc[0, -1]
        ctr_the_day_before_yesterday = df.iloc[1, -1]
        yesterday_date = df.date_time.dt.date[0]
        dau_yesterday = df.iloc[0, 1]
        dau_the_day_before_yesterday = df.iloc[1, 1]

        if dau_yesterday >= dau_the_day_before_yesterday:
            dau_change = 'больше'
        else:
            dau_change = 'меньше'

        if last_day_views >= penultimate_day_views:
            views_change = 'больше'
        else:
            views_change = 'меньше'

        if last_day_likes >= penultimate_day_likes:
            likes_change = 'больше'
        else:
            likes_change = 'меньше'
            
        if ctr_yesterday >= ctr_the_day_before_yesterday:
            ctr_change = 'больше'
        else:
            ctr_change = 'меньше'     

        message = f'''Ключевые метрики за вчерашний день ({yesterday_date}): 
              \n* DAU: {dau_yesterday}, на {round(dau_yesterday / dau_the_day_before_yesterday, 2)}% {dau_change} чем позавчера ({dau_the_day_before_yesterday}).
              \n* Просмотры: {last_day_views}, на {round(last_day_views / penultimate_day_views, 2)}% {views_change} чем позавчера ({penultimate_day_views}).
              \n* Лайки: {last_day_likes}, на {round(last_day_likes / penultimate_day_likes, 2)}% {likes_change} чем позавчера ({penultimate_day_likes}).
              \n* CTR: {round(ctr_yesterday, 4) * 100}%, на {round((ctr_yesterday / ctr_the_day_before_yesterday), 2)}% {ctr_change} чем позавчера ({round(ctr_the_day_before_yesterday * 100, 2)})'''

        return message

    @task()
    def form_plots(df):

        def form_lineplot(ax, x, y, title = None, xlabel = None, ylabel = None):
            plot = sns.lineplot(x, y, ax = ax)
            plot.set_title(title)
            plot.set_xlabel(xlabel)
            plot.set_ylabel(ylabel)
            
        fig, axs = plt.subplots(ncols = 2, nrows = 2)
        form_lineplot(axs[0][0], x = df.date_time, y = df.dau, title = 'DAU')
        form_lineplot(axs[0][1], x = df.date_time, y = df.ctr * 100, title = 'CTR')
        form_lineplot(axs[1][0], x = df.date_time, y = df.views, title = 'Views')
        form_lineplot(axs[1][1], x = df.date_time, y = df.likes, title = 'Likes')
        plt.suptitle('Данные за прошлую неделю', size = 20)

        plot_object = io.BytesIO()
        fig.savefig(plot_object)
        plot_object.seek(0)
        
        return plot_object
    
    @task()
    def send_report(chat_id, message, plot_object):
        bot.sendMessage(chat_id = chat_id, text = message)
        bot.sendPhoto(chat_id = chat_id, photo = plot_object)

    df = extract_data()
    message = transformation(df)
    plot_object = form_plots(df)
    send_report(chat_id, message, plot_object)
    
report_for_feed = report_for_feed()

