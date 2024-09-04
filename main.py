from google.cloud import bigquery
from google.colab import auth
import pandas as pd
from IPython.display import display
import re
import time


def connect_bq(pjid_new=None):
    global pjid

    auth.authenticate_user()
    print(
        f"""pjid: {pjid_new} authenticated at: {pd.Timestamp.now('Singapore').strftime('%Y-%m-%d %H:%M')}"""
    )
    pjid = pjid_new


def q(query):
    result = bigquery.Client(project=pjid).query(query)
    print(f"Job ID: {result.job_id}")
    start_time, minutes, seconds = time.time() + 28800, 0, 0
    dynamic_output = display("Query starting", display_id=True)
    while not result.done():
        minutes, seconds = divmod(int(time.time() + 28800 - start_time), 60)
        dynamic_output.update(
            f"Query running: {minutes}m {seconds}s since {time.strftime('%Y-%m-%d %H:%M', time.localtime(start_time))}"
        )
        time.sleep(0.5)
    dynamic_output.update(
        f"Query finished: {minutes}m {seconds}s from {time.strftime('%Y-%m-%d %H:%M', time.localtime(start_time))} to {time.strftime('%H:%M', time.localtime(time.time()+28800))}"
    )
    if result.errors:
        error_messages = "\n".join([error["message"] for error in result.errors])
        print(f"\x1b[31mQuery failed: {error_messages}\x1b[0m")
        match = re.search(r"\[(\d+):(\d+)\]", result.errors[0]["message"])
        if match:
            ln, pn = map(int, match.groups())
            for i, line in enumerate(query.split("\n")):
                if i + 1 >= ln - 5 and i + 1 < ln + 6:  # if i+1 in range(ln-5,ln+6):
                    print(
                        f"{i+1}: {line[:pn-1]}\x1b[31m{line[pn-1:]}\x1b[0m"
                        if i + 1 == ln
                        else f"{i+1}: {line}"
                    )
        raise Exception(error_messages)
    else:
        destination = (
            result.destination
            if result.ddl_target_table is None
            else result.ddl_target_table
        )
        print(f"\x1b[92mDestination: {destination}\x1b[0m")
        print(f"\x1b[93m{result.dml_stats}\x1b[0m") if result.dml_stats else None
        return result.to_dataframe()


def send_gchat(message, webhook):
    import json
    import requests

    headers = {"Content-Type": "application/json; charset=UTF-8"}
    data = json.dumps({"text": message})
    response = requests.post(webhook, headers=headers, data=data)
    if response.status_code == 400:
        raise Exception(f"Error sending message: {response.text}")
    return response.text
