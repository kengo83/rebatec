import threading
import datetime
from main import fetch_searched_page_num,run,no_keyword_run,fetch_searched_page_num_no_keyword
from log import set_logger
logger = set_logger(__name__)

dt_now = datetime.datetime.now()

def main(place,job,keyword,dir_name,csv_name): #キーワードが存在する場合の同時処理
    logger.info(f"{dt_now}:スクレイピングを開始します")
    page_num = fetch_searched_page_num(place,job,keyword)
    page_num_list = list(range(page_num))
    thread_list = []
    count = 1
    for id in page_num_list:
        t = threading.Thread(target=run,args=[place,job,keyword,count,id,dir_name,csv_name])
        t.start()
        thread_list.append(t)
        count += 1
    for thread in thread_list:
        thread.join()
    print('終了しました。')
    logger.info(f"{dt_now}:全ページ終了しました。")

def no_keyword_main(place,job,dir_name,csv_name): #キーワードが存在しない場合の同時処理
    page_num = fetch_searched_page_num_no_keyword(place,job)
    page_num_list = list(range(page_num))
    thread_list = []
    count = 1
    for id in page_num_list:
        t = threading.Thread(target=no_keyword_run,args=[place,job,count,id,dir_name,csv_name])
        t.start()
        thread_list.append(t)
        count += 1
    for thread in thread_list:
        thread.join()
    print('終了しました。')
    logger.info(f"{dt_now}:全ページ終了しました。")

