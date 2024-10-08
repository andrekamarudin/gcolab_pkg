import json
import re
import subprocess
import time
from datetime import datetime
from typing import Optional

import pandas as pd
import pytz
import requests
from colorama import Fore, Style
from google.cloud import bigquery
from google.colab import auth
from IPython.display import display


def connect_bq(pjid_new=None):
    """
    Authenticate the user and set the project ID for BigQuery.

    :param pjid_new: The new project ID to authenticate.

    :return: None
    """
    global pjid

    auth.authenticate_user()
    message = f"""pjid: {pjid_new} authenticated at: {pd.Timestamp.now('Singapore').strftime('%Y-%m-%d %H:%M')}"""
    cprint(message, color=Fore.GREEN, style=Style.BRIGHT)
    pjid = pjid_new


def q(query):
    """
    Execute a BigQuery query and return the result as a DataFrame.

    :param query: The BigQuery query to execute.

    :return: The result of the BigQuery query as a DataFrame.
    """
    result = bigquery.Client(project=pjid).query(query)
    cprint(f"Job ID: {result.job_id}", color=Fore.GREEN, style=Style.BRIGHT)
    start_time, start_datetime, minutes, seconds = (
        time.time() + 28800,
        datetime.now(pytz.timezone("Singapore")),
        0,
        0,
    )
    dynamic_output = display("Query starting", display_id=True)
    while not result.done():
        minutes, seconds = divmod(int(time.time() + 28800 - start_time), 60)
        dynamic_output.update(
            f"Query running: {minutes}m {seconds}s since {datetime.now(pytz.timezone('Singapore')):%Y-%m-%d %H:%M}"
        )
        time.sleep(0.5)
    dynamic_output.update(
        f"Query finished: {minutes}m {seconds}s from {start_datetime:%Y-%m-%d %H:%M} to {datetime.now(pytz.timezone('Singapore')):%H:%M}"
    )
    if result.errors:
        error_messages = "\n".join([error["message"] for error in result.errors])
        cprint(f"Query failed: {error_messages}", color=Fore.RED, style=Style.BRIGHT)
        match = re.search(r"\[(\d+):(\d+)\]", result.errors[0]["message"])
        if match:
            ln, pn = map(int, match.groups())
            for i, line in enumerate(query.split("\n")):
                if i + 1 >= ln - 5 and i + 1 < ln + 6:  # if i+1 in range(ln-5,ln+6):
                    cprint(
                        f"{i+1}: {line[:pn-1]}\x1b[31m{line[pn-1:]}\x1b[0m"
                        if i + 1 == ln
                        else f"{i+1}: {line}",
                        color=Fore.RED,
                        style=Style.BRIGHT,
                    )
        raise Exception(error_messages)
    else:
        destination = (
            result.destination
            if result.ddl_target_table is None
            else result.ddl_target_table
        )
        cprint(f"Destination: {destination}", color=Fore.GREEN, style=Style.BRIGHT)
        cprint(
            str(result.dml_stats), color=Fore.LIGHTYELLOW_EX, style=Style.BRIGHT
        ) if result.dml_stats else None
        return result.to_dataframe()


def send_gchat(
    message: str,
    webhook: str,
    footer: Optional[str] = None,
) -> str:
    """
    Send a message to a Google Chat webhook.

    :param message: The message to send.
    :param webhook: The URL of the Google Chat webhook.
    :param footer: The footer to add to the message.

    :return: The response from the Google Chat webhook.
    """
    headers = {"Content-Type": "application/json; charset=UTF-8"}
    if footer is None:
        footer = f"Sent by {get_user_email()} on {datetime.now(pytz.timezone('Singapore')):%Y-%m-%d %H:%M}"
    message = f"{message}\n\n{footer}"
    data = json.dumps({"text": message})
    response = requests.post(webhook, headers=headers, data=data)
    if response.status_code == 400:
        raise Exception(f"Error sending message: {response.text}")
    return response.text


def get_user_email():
    """
    Get the email of the user.

    :return: The email of the user.
    """
    shell_command = "gcloud auth print-access-token"
    gcloud_token = subprocess.check_output(shell_command, shell=True).decode().strip()
    gcloud_tokeninfo = requests.get(
        "https://www.googleapis.com/oauth2/v3/tokeninfo?access_token=" + gcloud_token
    ).json()
    return gcloud_tokeninfo["email"]


def cprint(*args, color=Fore.WHITE, style=Style.NORMAL, **kwargs):
    print(color + style + kwargs.get("sep", " ").join(args) + Style.RESET_ALL, **kwargs)
