import json
from datetime import datetime

VALID_DICT_KEYS = ["dt_from", "dt_upto","group_type"]
VALID_GROUP_TYPES = ["month", "day", "hour"]
VALID_DICT_LEN = len(VALID_DICT_KEYS)


def is_json_return(myjson):
  try:
    json_dc = json.loads(myjson)
  except ValueError:
    return False
  return json_dc


def is_isodate(date_string):
    try:
        datetime.fromisoformat(date_string)
    except ValueError:
        return False
    return True

def check_dictionary(dictionary):
  if len(dictionary) != VALID_DICT_LEN: return False
  for key in dictionary:
    if key not in VALID_DICT_KEYS:
      return False
  if dictionary["group_type"] not in VALID_GROUP_TYPES: return False
  return is_isodate(dictionary["dt_from"]) and is_isodate(dictionary["dt_upto"])


def if__data_valid_return_dict(json_message):
  json_dc = is_json_return(json_message)
  if not json_dc: return False
  if not check_dictionary(json_dc): return False
  return json_dc

  
   

if __name__ == "__main__":
  valid = json.dumps({
  "dt_from":"2022-09-01T00:00:00",
  "dt_upto":"2022-12-31T23:59:00",
  "group_type":"hour",
  })

  is_valid = if__data_valid_return_dict(valid)
  print(is_valid)


