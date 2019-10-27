''' This will scrape the site, weather.com, and upload it to a MongoDB database. '''


from splinter import Browser
# import funcs # this should be the file funcs.py

# Set the zip codes
codes = []
codes.append('27606')   # just hard code one in
code = codes[0]    # set the variable for zip_and_click
url = 'weather.com'

def chrome():
    '''Finds the chromedriver in the system and creates a Chrome browser'''
    import shutil
    executable_path = {'executable_path': shutil.which('chromedriver')}
    browser = Browser('chrome', **executable_path)
    return(browser)


def zip_and_click(code, browser):
    '''Enter zip codes into the search bar on weather.com and click the first result.
    Returns the browser at the first data page.
    '''
    inputs = browser.find_by_tag('input')
    inputs[0].fill(code)
    browser.click_link_by_partial_href('/weather/today/l')
    return(browser)


browser = chrome()
browser.visit(url)
zip_and_click(code, browser)
