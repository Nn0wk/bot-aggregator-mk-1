from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta  


def fill_empty_dates_with_zeroes(full_date_range, returnedDict={}):
    full_values = []
    for date in full_date_range:
        value = returnedDict.get(date)
        if value == None:
            full_values.append(0)
        else:
            full_values.append(value)
    return full_values


def chose_delta(group_by):
    reld = relativedelta()
    if group_by == "month":
        reld = relativedelta(months=1)
    elif group_by == "day":
        reld = relativedelta(days=1)
    elif group_by == "hour":
        reld = relativedelta(hours=1)
    return reld


def date_range(start_isodate, end_iso_date, group_by):
    step_date = chose_delta(group_by)
    print("STEPDATE:", step_date)
    start = datetime.fromisoformat(start_isodate)
    end = datetime.fromisoformat(end_iso_date)
    start_hour = start.hour + 1 if start.minute != 0 else 0
    start_date = start.replace(hour=start_hour, minute=0, second=0, microsecond=0)
    # end_date = end.replace(hour=0, minute=0, second=0, microsecond=0)
    time_labels_list = [start_date.isoformat()]
    current_date = start_date
    while True:
        current_date = current_date + step_date 
        if current_date <= end:
            time_labels_list.append(current_date.isoformat())
        else: break
    return time_labels_list
    




