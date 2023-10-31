from datetime import datetime



def month_pipeline(iso_date_from, iso_date_to):
    return [
    {
        '$match': {
            'dt': {
                '$gte': datetime.fromisoformat(iso_date_from),
                '$lte': datetime.fromisoformat(iso_date_to), 

            }
        }
    }, {
        '$project': {
            'y': {
                '$year': '$dt'
            }, 
            'm': {
                '$month': '$dt'
            }, 
            'dt': '$dt',
            'value': 1
        }
    }, {
        '$group': {
            '_id': {
                'year': '$y', 
                'month': '$m', 
            }, 
            'total_value': {
                '$sum': '$value'
            }, 
            'first': {
                '$min': '$dt'
            }
        }
    }, {
        '$sort': {
            '_id': 1
        }
    }
]


def day_pipeline(iso_date_from, iso_date_to):
    return [
    {
        '$match': {
            'dt': {
                '$gte': datetime.fromisoformat(iso_date_from),
                '$lte': datetime.fromisoformat(iso_date_to), 
            }
        }
    },
     {
        '$project': {
            'y': {
                '$year': '$dt'
            }, 
            'm': {
                '$month': '$dt'
            }, 
            'd': {
                '$dayOfMonth': '$dt'
            }, 
            'dt': '$dt',
            'value': 1
        }
    }, {
        '$group': {
            '_id': {
                'year': '$y', 
                'month': '$m', 
                'day': '$d', 
            }, 
            'total_value': {
                '$sum': '$value'
            }, 
            'first': {
                '$min': '$dt'
            }
        }
    }, {
        '$sort': {
            '_id': 1
        }
    }
]


def hour_pipeline(iso_date_from, iso_date_to):
    return [
    {
        '$match': {
            'dt': {
                '$gte': datetime.fromisoformat(iso_date_from),
                '$lte': datetime.fromisoformat(iso_date_to), 
            }
        }
    },
     {
        '$project': {
            'y': {
                '$year': '$dt'
            }, 
            'm': {
                '$month': '$dt'
            }, 
            'd': {
                '$dayOfMonth': '$dt'
            }, 
            'h': {
                '$hour': '$dt'
            }, 
            'dt': '$dt',
            'value': 1
        }
    }, {
        '$group': {
            '_id': {
                'year': '$y', 
                'month': '$m', 
                'day': '$d', 
                'hour': '$h'
            }, 
            'total_value': {
                '$sum': '$value'
            }, 
            'first': {
                '$min': '$dt'
            }
        }
    }, {
        '$sort': {
            '_id': 1
        }
    }
]