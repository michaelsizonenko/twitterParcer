import json
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from csv import DictWriter
from datetime import datetime, timedelta

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import sys

LOCK = threading.Lock()


def scroll_to_bottom(driver: webdriver.Chrome):
    '''
    This function scrolls down to the bottom of the webpage
    ---
    driver: webdriver.Chrome - the Chrome driver object to control the browser
    '''
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

def get_body_heigth(driver: webdriver.Chrome) -> int:
    '''
    This function returns the height of the body of the webpage
    ---
    driver: webdriver.Chrome - the Chrome driver object to control the browser
    returns: int - the height of the webpage's body
    '''
    return driver.execute_script("return document.body.scrollHeight")

def _scrap(driver: webdriver.Chrome) -> list[list[str]]:
    '''
    This is an internal function that scrapes tweet data from Twitter
    ---
    driver: webdriver.Chrome - the Chrome driver object to control the browser
    returns: list[list[str]] - a list of lists containing the scraped tweet data
    '''
    js_scrap_script = ''' const all_data = [];
        const all_tweets = document.getElementsByTagName("section")[0].childNodes[1].childNodes[0];

        for (let i = 0; i < all_tweets.childElementCount; i++) {
            const single_tweet_data = []
            const text = document.getElementsByTagName("section")[0].childNodes[1].childNodes[0].childNodes[i].childNodes[0].childNodes[0].childNodes[0].childNodes[0].childNodes[0].childNodes[1].childNodes[1].childNodes[1].childNodes[0].textContent;
            const user_name = document.getElementsByTagName("section")[0].childNodes[1].childNodes[0].childNodes[i].childNodes[0].childNodes[0].childNodes[0].childNodes[0].childNodes[0].childNodes[1].childNodes[1].childNodes[0].childNodes[0].childNodes[0].childNodes[0].childNodes[0].childNodes[1].childNodes[0].childNodes[0].textContent;
            let date = document.getElementsByTagName("section")[0].childNodes[1].childNodes[0].childNodes[i].childNodes[0].childNodes[0].childNodes[0].childNodes[0].childNodes[0].childNodes[1].childNodes[1].childNodes[0].childNodes[0].childNodes[0].childNodes[0].childNodes[0].childNodes[1].childNodes[0].childNodes[2].childNodes[0].childNodes[0].getAttribute("datetime");
            if (date === null) {
            date = document.getElementsByTagName("section")[0].childNodes[1].childNodes[0].childNodes[i].childNodes[0].childNodes[0].childNodes[0].childNodes[0].childNodes[0].childNodes[1].childNodes[1].childNodes[0].childNodes[0].childNodes[0].childNodes[0].childNodes[0].childNodes[1].childNodes[0].childNodes[2].childNodes[0].childNodes[0].childNodes[0].getAttribute("datetime");
        }
            let url_with_id = document.getElementsByTagName("section")[0].childNodes[1].childNodes[0].childNodes[i].childNodes[0].childNodes[0].childNodes[0].childNodes[0].childNodes[0].childNodes[1].childNodes[1].childNodes[0].childNodes[0].childNodes[0].childNodes[0].childNodes[0].childNodes[1].childNodes[0].childNodes[2].childNodes[0].getAttribute("href");
            if (url_with_id === null) {
            url_with_id = document.getElementsByTagName("section")[0].childNodes[1].childNodes[0].childNodes[i].childNodes[0].childNodes[0].childNodes[0].childNodes[0].childNodes[0].childNodes[1].childNodes[1].childNodes[0].childNodes[0].childNodes[0].childNodes[0].childNodes[0].childNodes[1].childNodes[0].childNodes[2].childNodes[0].childNodes[0].getAttribute("href");
        }
            const verif_path = document.getElementsByTagName("section")[0].childNodes[1].childNodes[0].childNodes[i].getElementsByTagName("svg")[0].childNodes[0].childNodes[0].getAttribute("d");
            
                single_tweet_data.push(text);
                single_tweet_data.push(user_name);
                single_tweet_data.push(date);
                single_tweet_data.push(url_with_id);
                single_tweet_data.push(verif_path);
                all_data.push(single_tweet_data); 
        }
        return all_data; '''
    try:
        tweets_on_page = driver.execute_script(js_scrap_script)
    except:
        tweets_on_page = []
    return tweets_on_page

def transfom_date_time(date_time: str) -> str:
    '''
    This function transforms a date-time string into a formatted date string
    ---
    date_time: str - the date-time string to transform
    returns: str - the formatted date string
    '''
    date_object = datetime.fromisoformat(date_time.replace('Z', '+00:00'))
    formatted_date = date_object.strftime('%Y-%m-%d %H:%M:%S')

    return formatted_date

def update_csv(data: list[list[str]]):
    '''
    This function updates a CSV file with scraped tweet data
    ---
    data: list[list[str]] - a list of lists containing the scraped tweet data
    '''
    verif_checkmark_path = 'M20.396 11c-.018-.646-.215-1.275-.57-1.816-.354-.54-.852-.972-1.438-1.246.223-.607.27-1.264.14-1.897-.131-.634-.437-1.218-.882-1.687-.47-.445-1.053-.75-1.687-.882-.633-.13-1.29-.083-1.897.14-.273-.587-.704-1.086-1.245-1.44S11.647 1.62 11 1.604c-.646.017-1.273.213-1.813.568s-.969.854-1.24 1.44c-.608-.223-1.267-.272-1.902-.14-.635.13-1.22.436-1.69.882-.445.47-.749 1.055-.878 1.688-.13.633-.08 1.29.144 1.896-.587.274-1.087.705-1.443 1.245-.356.54-.555 1.17-.574 1.817.02.647.218 1.276.574 1.817.356.54.856.972 1.443 1.245-.224.606-.274 1.263-.144 1.896.13.634.433 1.218.877 1.688.47.443 1.054.747 1.687.878.633.132 1.29.084 1.897-.136.274.586.705 1.084 1.246 1.439.54.354 1.17.551 1.816.569.647-.016 1.276-.213 1.817-.567s.972-.854 1.245-1.44c.604.239 1.266.296 1.903.164.636-.132 1.22-.447 1.68-.907.46-.46.776-1.044.908-1.681s.075-1.299-.165-1.903c.586-.274 1.084-.705 1.439-1.246.354-.54.551-1.17.569-1.816zM9.662 14.85l-3.429-3.428 1.293-1.302 2.072 2.072 4.4-4.794 1.347 1.246z'
    
    global LOCK
    LOCK.acquire()
    with open("twitterData.csv", "a+", encoding='utf-8', newline='') as file:
        fieldnames = ['Time','Text', 'Name', 'Tweet_id', 'Verified']
        writer = DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for tweet in data:
            writer.writerow({
                'Time': transfom_date_time(tweet[2]),
                'Text': tweet[0].replace('\n', ' '),
                'Name': tweet[1],
                'Tweet_id': tweet[3],
                'Verified': tweet[4] == verif_checkmark_path
            })
    LOCK.release()


def get_url_by_date(start_date: str) -> str:
    '''
    This function creates a search URL for Twitter given a starting date
    ---
    start_date: str - the starting date to search for tweets in the format YYYY-MM-DD
    returns: str - the URL for the Twitter search
    '''
    end_date = next_day_srting(start_date)
    # url = f'https://twitter.com/search?f=live&q=(%23bitcoin)%20lang%3Aen%20until%3A{end_date}%20since%3A{start_date}%20-filter%3Areplies&src=typed_query'
    # url = f"https://twitter.com/search?q=(%23bitcoin)%20lang%3Aen%20until%3A{end_date}%20since%3A{start_date}%20-filter%3Areplies&src=typeahead_click"
    # url = f'https://twitter.com/search?q=(%23bitcoin)%20min_replies%3A10%20min_retweets%3A10%20lang%3Aen%20until%3A{end_date}%20since%3A{start_date}&src=typed_query&f=live'
    url = f'https://twitter.com/search?q=%23bitcoin%20min_retweets%3A10%20lang%3Aen%20until%3A{end_date}%20since%3A{start_date}&src=typed_query&f=live'
    
    return url

def get_date_to_scrap() -> list[str]:
    '''
    This function gets a list of dates to scrape from a file of missing hours
    ---
    returns: list[str] - a list of dates to scrape
    '''
    with open('missing_hours.csv', 'r') as f:
        dates = set()
        for line in f:
            line = line.strip()
            dates.add(line.split(' ')[0])
    
    return list(dates)

def next_day_srting(today: str) -> str:
    '''
    This function calculates the string representation of the next day given a starting date
    ---
    today: str - the starting date in the format YYYY-MM-DD
    returns: str - the next day in the format YYYY-MM-DD
    '''
    date_object = datetime.strptime(today, '%Y-%m-%d')
    next_day_object = date_object + timedelta(days=1)
    next_day_string = next_day_object.strftime('%Y-%m-%d')

    return next_day_string

def scrap(driver: webdriver.Chrome, time_to_scrap: int):
    '''
    This function scrapes Twitter for a given amount of time and saves the tweets to a CSV file
    ---
    driver: webdriver.Chrome - the Chrome driver object used for scraping
    time_to_scrap: int - the number of seconds to scrape Twitter for
    '''
    all_tweets_to_save = []
    buffer = []

    current_height = get_body_heigth(driver)
    last_height = 0
    counter = 0


    start_time = time.time()
    while(time.time() - start_time < time_to_scrap):
        time.sleep(.5)
        current_tweets = _scrap(driver)
        tweets_to_save = [tweet for tweet in current_tweets if tweet not in buffer]
        if current_tweets:
            buffer = current_tweets
        all_tweets_to_save.extend(tweets_to_save)
        scroll_to_bottom(driver)

        last_height = current_height
        current_height = get_body_heigth(driver)
        if last_height == current_height:
            counter += 1
        else:
            counter = 0

        if counter > 10:
            break

    update_csv(all_tweets_to_save)


def start_driver():
    try:
        options = Options()
        options.headless = False

        # Disabling chrome's bot mode
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option(
            "excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        prefs = {
            "translate_whitelists": {"uk": "en"},
            "translate": {"enabled": "True"}
        }
        options.add_experimental_option("prefs", prefs)
        options.add_argument(
            f'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        options.add_argument('--no-sandbox')
        options.add_argument('--window-size=1420, 1200')
        options.add_argument('--disable-gpu')
        
        driver = webdriver.Chrome(
            executable_path='./chromedriver',
            options=options
        )
    except Exception as e:
        print('Failed to run webdriver(check webdriver version and if it\' in the same dir with script\n\n)')
        sys.exit(1)

    return driver

def multithred_func(day: str):
    '''
    This function scrapes Twitter for a single day in a separate thread
    ---
    day: str - the date to scrape tweets for, in format 'YYYY-MM-DD'
    '''
    with open('scraped_date.json', 'r') as f:
        already_scraped_days: list = json.load(f)

    if day in already_scraped_days:
        return

    url_to_scrap = get_url_by_date(day)

    global LOCK

    LOCK.acquire()
    driver = start_driver()
    time.sleep(5 + random.random())
    LOCK.release()


    driver.get(url_to_scrap)

    scrap(driver, 60*10)

    driver.close()


    LOCK.acquire()
    with open('scraped_date.json', 'r') as f:
        already_scraped_days: list = json.load(f)

    already_scraped_days.append(day)

    with open('scraped_date.json', 'w') as f:
        json.dump(already_scraped_days, f)
    LOCK.release()

    print(f'already scraped days:  {len(already_scraped_days)}')



def main():
    dates = get_date_to_scrap()

    print(f'number of days to scrap: {len(dates)}')

    with ThreadPoolExecutor(4) as executor:
        executor.map(multithred_func, dates)



if __name__ == "__main__":
    main()