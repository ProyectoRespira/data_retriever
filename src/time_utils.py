from pytz import timezone

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