from pytz import timezone
from datetime import datetime
import re 

def convert_to_local_time(time_utc):
    local_time = timezone('America/Asuncion')
    utc_minus_0 = timezone('UTC')
    time_utc = utc_minus_0.localize(time_utc)
    time_utc_local = time_utc.astimezone(local_time)
    return time_utc_local.replace(tzinfo=None)

def convert_to_utc(time_local):
    local_time = timezone('America/Asuncion')
    time_localized = local_time.localize(time_local)
    utc_time = timezone('UTC')
    time_local_utc = time_localized.astimezone(utc_time)
    return time_local_utc.replace(tzinfo=None)

def validate_date_hour(date, hour):
    if date is None:
        return False
    if hour is None:
        return False
    
    date = date.strip()
    hour = hour.strip()

    date_pattern = re.compile(r'^\d{1,2}-\d{1,2}-\d{4}$')
    hour_pattern = re.compile(r'^\d{1,2}:\d{2}$')

    if not date_pattern.match(date):
        return False
    if not hour_pattern.match(hour):
        return False
    try:
        date_obj = datetime.strptime(date, '%d-%m-%Y')
        if date_obj.year < 2019:
            return False
    except ValueError:
        return False
    
    return True