import os
from selenium.webdriver import Chrome, ChromeOptions
import pandas as pd
import datetime
import math
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from webdriver_manager.chrome import ChromeDriverManager
from time import sleep,time
import threading
import eel
from log import set_logger
logger = set_logger(__name__)

EXP_CSV_PATH = "./{dir_name}/{csv_name}"
MYNAVI_SEARCH_URL = "https://tenshoku.mynavi.jp/list/{place}/{job}/kw{keyword}/"
MYNAVI_SEARCH_PAGE_URL =  "https://tenshoku.mynavi.jp/list/{place}/{job}/kw{keyword}/pg{page}/"
MYNAVI_NO_KEYWORD_URL = "https://tenshoku.mynavi.jp/list/{place}/{job}/"
MYNAVI_NO_KEYWORD_PAGE_URL = "https://tenshoku.mynavi.jp/list/{place}/{job}/pg{page}/"

dt_now = datetime.datetime.now()


### Chromeを起動する関数
def set_driver(driver_path, headless_flg):
    # Chromeドライバーの読み込み
    options = ChromeOptions()

    # ヘッドレスモード（画面非表示モード）をの設定
    if headless_flg == True:
        options.add_argument('--headless')

    # 起動オプションの設定
    options.add_argument(
        '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36')
    # options.add_argument('log-level=3')
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--ignore-ssl-errors')
    options.add_argument('--incognito')          # シークレットモードの設定を付与

    # ChromeのWebDriverオブジェクトを作成する。
    driver_path = ChromeDriverManager().install()
    return Chrome(driver_path,options=options)

def find_table_target_word(th_elms, td_elms, target:str): #td_elmsをtd内の<div class='text'>内の内容にする
    for th,td in zip(th_elms, td_elms):
        if th.text == target:
            return td.text
    
def fetch_searched_page_num(place,job,keyword): #キーワードがある場合のページ数を取得
    if os.name == 'nt': #Windows
        driver = set_driver("chromedriver.exe", False)
    elif os.name == 'posix': #Mac
        driver = set_driver("chromedriver", False)
    driver.get(MYNAVI_SEARCH_PAGE_URL.format(place=place,job=job,keyword=keyword, page=1))
    sleep(1)
    # ポップアップを閉じる
    driver.execute_script('document.querySelector(".karte-close").click()')
    sleep(1)
    # ポップアップを閉じる
    driver.execute_script('document.querySelector(".karte-close").click()')
    sleep(8)
    if len(driver.find_elements_by_css_selector('.pager.pagerMargin'))>=1:
        return math.ceil(int(driver.find_element_by_css_selector(".total_txt.total_num").text) / 50)
    elif len(driver.find_elements_by_class_name('pager'))>=1:
        return math.ceil(int(driver.find_element_by_css_selector(".result__num>em").text) / 50)

def fetch_searched_page_num_no_keyword(place,job): #キーワードがない場合のページ数を取得
    if os.name == 'nt': #Windows
        driver = set_driver("chromedriver.exe", False)
    elif os.name == 'posix': #Mac
        driver = set_driver("chromedriver", False)
    driver.get(MYNAVI_NO_KEYWORD_PAGE_URL.format(place=place,job=job,page=1))
    sleep(1)
    # ポップアップを閉じる
    driver.execute_script('document.querySelector(".karte-close").click()')
    sleep(1)
    # ポップアップを閉じる
    driver.execute_script('document.querySelector(".karte-close").click()')
    sleep(8)
    if len(driver.find_elements_by_css_selector('.pager.pagerMargin'))>=1:
        return math.ceil(int(driver.find_element_by_css_selector(".total_txt.total_num").text) / 50)
    elif len(driver.find_elements_by_class_name('pager'))>=1:
        return math.ceil(int(driver.find_element_by_css_selector(".result__num>em").text) / 50)

semaphore = threading.BoundedSemaphore(3)  # 5個のスレッドの同時処理を許容する

def run(place,job,keyword,page,thread_num,dir_name,csv_name): #keywordが検索条件に含まれる場合の処理
    semaphore.acquire()
    sleep(1)
    os.makedirs(dir_name,exist_ok=True)
    if os.name == 'nt': #Windows
        driver = set_driver("chromedriver.exe", False)
    elif os.name == 'posix': #Mac
        driver = set_driver("chromedriver", False)
    driver.get(MYNAVI_SEARCH_PAGE_URL.format(place=place,job=job,keyword=keyword,page=page))
    sleep(1)
    # ポップアップを閉じる
    driver.execute_script('document.querySelector(".karte-close").click()')
    sleep(1)
    # ポップアップを閉じる
    driver.execute_script('document.querySelector(".karte-close").click()')
    df = pd.DataFrame()
    link_list=[]
    companys = driver.find_elements_by_class_name('recruit')
    for company in companys: #リンクリストを作成
        a = company.find_elements_by_css_selector('.link.entry_click.entry3')
        link = a[0].get_attribute('href')
        link_list.append(link)
    print(link_list)
    for l in link_list:
        driver.get(l)
        sleep(2)
        if len(driver.find_elements_by_class_name("jobInterviewBox"))>=1:
            try:
                company_name = driver.find_element_by_class_name('companyName') #会社名
                job_content = driver.find_element_by_class_name('jobPointArea__body') #仕事内容
                worker = driver.find_element_by_class_name('jobPointArea__body--large') #対象となる方
                offer_table = driver.find_element_by_class_name('jobOfferTable') #募集要項の表を取り出す
                company_table = driver.find_element_by_css_selector('.jobOfferTable.thL') #会社情報の表を取り出す
                        
                koyou = find_table_target_word(offer_table.find_elements_by_class_name('jobOfferTable__head'),offer_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'雇用形態')
                time = find_table_target_word(offer_table.find_elements_by_class_name('jobOfferTable__head'),offer_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'勤務時間')
                pay =  find_table_target_word(offer_table.find_elements_by_class_name('jobOfferTable__head'),offer_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'給与')
                rest = find_table_target_word(offer_table.find_elements_by_class_name('jobOfferTable__head'),offer_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'休日・休暇')
                support = find_table_target_word(offer_table.find_elements_by_class_name('jobOfferTable__head'),offer_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'福利厚生')
                money = find_table_target_word(company_table.find_elements_by_class_name('jobOfferTable__head'),company_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'資本金')
                profit = find_table_target_word(company_table.find_elements_by_class_name('jobOfferTable__head'),company_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'売上高')
                try:
                    df = df.append({
                        '会社名':company_name.text,
                        '仕事内容':job_content.text,
                        '対象の方':worker.text,
                        '雇用形態':koyou,
                        '勤務時間':time,
                        '給与':pay,
                        '休日・休暇':rest,
                        '福利厚生':support,
                        '資本金':money, 
                        '売上高':profit
                    },ignore_index=True)
                    eel.view_company_name(company_name.text)
                    logger.info(f'[thread{thread_num + 1}]:成功しました{company_name.text}')
                except Exception as e:
                    eel.view_company_name('dataframe化失敗')
                    logger.error(f'[thread{thread_num + 1}]:{company_name.text} / {e}')
            except Exception as e:
                logger.error(f'[thread{thread_num + 1}]:失敗しました / {e}')
                eel.view_company_name('失敗しました')
        elif len(driver.find_elements_by_class_name("messageImgArea"))>=1:
            driver.find_element_by_css_selector(".tabNaviRecruit__list>.tabNaviRecruit__item:first-child>a").click() #求人詳細ボタンをクリック
            sleep(2)
            try:
                company_name = driver.find_element_by_class_name('companyName') #会社名
                job_content = driver.find_element_by_class_name('jobPointArea__head') #仕事内容
                worker = driver.find_element_by_class_name('jobPointArea__body--large') #対象となる方
                offer_table = driver.find_element_by_class_name('jobOfferTable') #募集要項の表を取り出す
                company_table = driver.find_element_by_css_selector('.jobOfferTable.thL') #会社情報の表を取り出す

                koyou = find_table_target_word(offer_table.find_elements_by_class_name('jobOfferTable__head'),offer_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'雇用形態')
                time = find_table_target_word(offer_table.find_elements_by_class_name('jobOfferTable__head'),offer_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'勤務時間')
                pay = find_table_target_word(offer_table.find_elements_by_class_name('jobOfferTable__head'),offer_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'給与')
                rest = find_table_target_word(offer_table.find_elements_by_class_name('jobOfferTable__head'),offer_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'休日・休暇')
                support = find_table_target_word(offer_table.find_elements_by_class_name('jobOfferTable__head'),offer_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'福利厚生')
                money = find_table_target_word(company_table.find_elements_by_class_name('jobOfferTable__head'),company_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'資本金')
                profit = find_table_target_word(company_table.find_elements_by_class_name('jobOfferTable__head'),company_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'売上高')      
                
                try:
                    df = df.append({
                        '会社名':company_name.text,
                        '仕事内容':job_content.text,
                        '対象の方':worker.text,
                        '雇用形態':koyou,
                        '勤務時間':time,
                        '給与':pay,
                        '休日・休暇':rest,
                        '福利厚生':support,
                        '資本金':money,
                        '売上高':profit
                    },ignore_index=True)
                    logger.info(f'[thread{thread_num + 1}]:成功しました{company_name.text}')
                    eel.view_company_name(company_name.text)
                except Exception as e:
                    logger.error(f'[thread{thread_num + 1}]:{company_name.text} / {e}')
                    eel.view_company_name('dataframe化失敗')
            except Exception as e:
                logger.error(f'[thread{thread_num + 1}]:失敗しました / {e}')
                eel.view_company_name('失敗しました')
    header = False if os.path.exists(EXP_CSV_PATH.format(dir_name=dir_name,csv_name=csv_name))else True
    df.to_csv(EXP_CSV_PATH.format(dir_name=dir_name,csv_name=csv_name),mode='a',index=False,header=header,encoding='utf_8_sig')
    driver.quit()
    logger.info(f'[thread{thread_num + 1}]:{dt_now}.終了します')
    semaphore.release()

def no_keyword_run(place,job,page,thread_num,dir_name,csv_name): #検索条件にキーワードが含まれていない場合の処理。151文と235文のURLの変数が変わっているだけでその他はrun関数と一緒
    semaphore.acquire()
    sleep(1)
    os.makedirs(dir_name,exist_ok=True)
    if os.name == 'nt': #Windows
        driver = set_driver("chromedriver.exe", False)
    elif os.name == 'posix': #Mac
        driver = set_driver("chromedriver", False)
    driver.get(MYNAVI_NO_KEYWORD_PAGE_URL.format(place=place,job=job,page=page))
    sleep(1)
    # ポップアップを閉じる
    driver.execute_script('document.querySelector(".karte-close").click()')
    sleep(1)
    # ポップアップを閉じる
    driver.execute_script('document.querySelector(".karte-close").click()')
    df = pd.DataFrame()
    link_list=[]
    companys = driver.find_elements_by_class_name('recruit')
    for company in companys: #リンクリストを作成
        a = company.find_elements_by_css_selector('.link.entry_click.entry3')
        link = a[0].get_attribute('href')
        link_list.append(link)
    print(link_list)
    for l in link_list:
        driver.get(l)
        sleep(2)
        if len(driver.find_elements_by_class_name("jobInterviewBox"))>=1:
            try:
                company_name = driver.find_element_by_class_name('companyName') #会社名
                job_content = driver.find_element_by_class_name('jobPointArea__body') #仕事内容
                worker = driver.find_element_by_class_name('jobPointArea__body--large') #対象となる方
                offer_table = driver.find_element_by_class_name('jobOfferTable') #募集要項の表を取り出す
                company_table = driver.find_element_by_css_selector('.jobOfferTable.thL') #会社情報の表を取り出す
                        
                koyou = find_table_target_word(offer_table.find_elements_by_class_name('jobOfferTable__head'),offer_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'雇用形態')
                time = find_table_target_word(offer_table.find_elements_by_class_name('jobOfferTable__head'),offer_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'勤務時間')
                pay = find_table_target_word(offer_table.find_elements_by_class_name('jobOfferTable__head'),offer_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'給与')
                rest = find_table_target_word(offer_table.find_elements_by_class_name('jobOfferTable__head'),offer_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'休日・休暇')
                support = find_table_target_word(offer_table.find_elements_by_class_name('jobOfferTable__head'),offer_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'福利厚生')
                money = find_table_target_word(company_table.find_elements_by_class_name('jobOfferTable__head'),company_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'資本金')
                profit = find_table_target_word(company_table.find_elements_by_class_name('jobOfferTable__head'),company_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'売上高')

                try:
                    df = df.append({
                        '会社名':company_name.text,
                        '仕事内容':job_content.text,
                        '対象の方':worker.text,
                        '雇用形態':koyou,
                        '勤務時間':time,
                        '給与':pay,
                        '休日・休暇':rest,
                        '福利厚生':support,
                        '資本金':money,
                        '売上高':profit
                    },ignore_index=True)
                    logger.info(f'[thread{thread_num + 1}]:成功しました{company_name.text}')
                    eel.view_company_name(company_name.text)
                except Exception as e:
                    logger.error(f'[thread{thread_num + 1}]:{company_name.text} / {e}')
                    eel.view_company_name('dataframe化失敗')
            except Exception as e:
                logger.error(f'[thread{thread_num + 1}]:失敗しました / {e}')
                eel.view_company_name('失敗しました')
        elif len(driver.find_elements_by_class_name("messageImgArea"))>=1:
            driver.find_element_by_css_selector(".tabNaviRecruit__list>.tabNaviRecruit__item:first-child>a").click() #求人詳細ボタンをクリック
            sleep(2)
            try:
                company_name = driver.find_element_by_class_name('companyName') #会社名
                job_content = driver.find_element_by_class_name('jobPointArea__head') #仕事内容
                worker = driver.find_element_by_class_name('jobPointArea__body--large') #対象となる方
                offer_table = driver.find_element_by_class_name('jobOfferTable') #募集要項の表を取り出す
                company_table = driver.find_element_by_css_selector('.jobOfferTable.thL') #会社情報の表を取り出す
                        
                koyou = find_table_target_word(offer_table.find_elements_by_class_name('jobOfferTable__head'),offer_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'雇用形態')
                time = find_table_target_word(offer_table.find_elements_by_class_name('jobOfferTable__head'),offer_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'勤務時間')
                pay = find_table_target_word(offer_table.find_elements_by_class_name('jobOfferTable__head'),offer_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'給与')
                rest = find_table_target_word(offer_table.find_elements_by_class_name('jobOfferTable__head'),offer_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'休日・休暇')
                support = find_table_target_word(offer_table.find_elements_by_class_name('jobOfferTable__head'),offer_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'福利厚生')
                money = find_table_target_word(company_table.find_elements_by_class_name('jobOfferTable__head'),company_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'資本金')
                profit = find_table_target_word(company_table.find_elements_by_class_name('jobOfferTable__head'),company_table.find_elements_by_css_selector('.jobOfferTable__body>.text:first-of-type'),'売上高')

                try:
                    df = df.append({
                        '会社名':company_name.text,
                        '仕事内容':job_content.text,
                        '対象の方':worker.text,
                        '雇用形態':koyou,
                        '勤務時間':time,
                        '給与':pay,
                        '休日・休暇':rest,
                        '福利厚生':support,
                        '資本金':money,
                        '売上高':profit
                        },ignore_index=True)
                    logger.info(f'[thread{thread_num + 1}]:成功しました{company_name.text}')
                    eel.view_company_name(company_name.text)
                except Exception as e:
                    logger.error(f'[thread{thread_num + 1}]:{company_name.text} / {e}')
                    eel.view_company_name('dataframe化失敗')
            except Exception as e:
                logger.error(f'[thread{thread_num + 1}]:失敗しました / {e}')
                eel.view_company_name('失敗しました')
    header = False if os.path.exists(EXP_CSV_PATH.format(dir_name=dir_name,csv_name=csv_name))else True #EXP_CSV_PATHが存在する場合Falseを返す。存在しない場合はTrueを返す
    df.to_csv(EXP_CSV_PATH.format(dir_name=dir_name,csv_name=csv_name),mode='a',index=False,header=header,encoding='utf_8_sig')
    driver.quit()
    logger.info(f'[thread{thread_num + 1}]:{dt_now}.終了します')
    semaphore.release()



# if __name__ == "__main__":
#     no_keyword_run('p11+','o11+','ramen.csv')
