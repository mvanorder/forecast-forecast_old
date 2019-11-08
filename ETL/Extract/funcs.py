''' Functions that will be used in this app...
'''
import shutil
from splinter import Browser
from selenium.common.exceptions import InvalidElementStateException
from selenium.common.exceptions import ElementNotInteractableException
import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure


host = 'local'
port = 27017


def chrome():
    ''' Finds the chromedriver in the system and creates a Chrome browser'''
    executable_path = {'executable_path': shutil.which('chromedriver')}
    browser = Browser('chrome', **executable_path)
    return(browser)


def zip_and_click(code):
    '''
    Enter zip codes into the search bar on weather.com and click the first result.
    '''
    import time
    
    filled = False
    clicked = False
    inputs = browser.find_by_tag('input') # get the search box reference
    search_box = inputs[0]
    
    while not filled:
        try:
            search_box.fill(code)
            filled = True
        except InvalidElementStateException:
            time.sleep(1)
    while not clicked:
        try:
            browser.click_link_by_partial_href('/weather/today/l')
            clicked = True
        except ElementNotInteractableException:
            time.sleep(1)
    return()


def check_db_access(str: host, port):
    '''A check that there is write access to the database'''
   
    client = MongoClient(host=host, port=port)
    try:
        # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster')
        print('Connection made')
    except ConnectionFailure:
        print("Server not available")

    # check the database connections
        # Get a count of the number of databases at the connection (accessible through that port) before attempting to add to it
    db_count_pre = len(client.list_database_names())
        # Add a database and collection
    db = client.test_db
    col = db.test_col

    # Insert something to the db
    post = {'name':'Chuck VanHoff',
           'age':'38',
           'hobby':'gardening'
           }
    col.insert_one(post)

        # Get a count of the databases after adding one
    db_count_post = len(client.list_database_names())

    if db_count_pre-db_count_post>=0:
        print('Your conneciton is flipped up')
    else:
        print('You have write access')

    client.drop_database(db)
    client.close()
    return()