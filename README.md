# forecast-forecast
Weather collection

This is intended to create a model to predict the errors in the weather prediction models.

## Description

## How to use it

### What you will need
You will need a file that is a comma separated list of locations, formatted as either:
coordinate pairs in dict form- example: {'lon': <(-90, 90)>, 'lat': <(-180, 180)>}
or a valid US zipcode in string format: '27006'
You will also need write access to a MongoDB database. *The database name can be set in
the config.py file.*

### What you will do
Open the get_and_make.py file to find the variable 'filename'. Edit it with the path to the
list of locations.
Now run get_and_make.py. That's it! Now you are collecting data from the OWM api and
loading it to the local database you have setup.
