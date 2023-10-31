
def compare_isodate_strings(one_isodatestr, two_isodatestr, group_by):
    if group_by == "month":
        substr_len  = 7
    elif group_by == "day":
        substr_len  = 10
    elif group_by == "hour":
        substr_len  = 13
    else: return False
    return one_isodatestr[0:substr_len] == two_isodatestr[0:substr_len]

def process_data(generated_dates, aggregated_dates, aggregated_values, group_by):
    processed_values = [0] * len(generated_dates)
    gen_dates_idx = -1 
    gen_dates_max_idx = len(generated_dates) - 1
    for aggr_idx in range(len(aggregated_dates)):
        while gen_dates_idx <= gen_dates_max_idx:
            gen_dates_idx += 1
            if compare_isodate_strings(aggregated_dates[aggr_idx], generated_dates[gen_dates_idx], group_by):
                processed_values[gen_dates_idx] = aggregated_values[aggr_idx]
                break
    return {
        "dataset": processed_values,
        "labels": generated_dates
    }

            

if __name__ == "__main__":
    date1 = "2022-10-01T00:00:00"
    date2 = "2022-10-03T20:06:00"
    result1 = compare_isodate_strings(date1, date2, "day")
    print(result1)
