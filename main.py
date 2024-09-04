# @title set up
# @title Create auth function
from google.cloud import bigquery
import pandas as pd
def auth(pjid_new=None):
  global pjid
  from google.colab import auth
  pjid = "fairprice-bigquery"  # @param {type: "string"}
  pjid_new = pjid if pjid_new is None else pjid_new
  auth.authenticate_user()
  client = bigquery.Client(project=pjid_new)
  print(f"""pjid: {pjid_new} authenticated at: {pd.Timestamp.now('Singapore').strftime('%Y-%m-%d %H:%M')}""")
  %reload_ext google.colab.data_table
auth()

# @title Create Q function
def q(query):
  from IPython.display import display
  import re
  import time
  result = bigquery.Client(project=pjid).query(query)
  print(f"Job ID: {result.job_id}")
  start_time, minutes, seconds = time.time() + 28800, 0, 0
  dynamic_output = display(f"Query starting", display_id=True)
  while not result.done():
    minutes, seconds = divmod(int(time.time() + 28800 - start_time), 60)
    dynamic_output.update(f"Query running: {minutes}m {seconds}s since {time.strftime('%Y-%m-%d %H:%M', time.localtime(start_time))}")
    time.sleep(0.5)
  dynamic_output.update(f"Query finished: {minutes}m {seconds}s from {time.strftime('%Y-%m-%d %H:%M', time.localtime(start_time))} to {time.strftime('%H:%M', time.localtime(time.time()+28800))}")
  if result.errors:
    error_messages = "\n".join([error["message"] for error in result.errors])
    print(f"\x1b[31mQuery failed: {error_messages}\x1b[0m")
    match = re.search(r"\[(\d+):(\d+)\]", result.errors[0]["message"])
    if match:
      ln,pn = map(int, match.groups())
      for i,line in enumerate(query.split("\n")):
        if i+1 >= ln-5 and i+1 < ln+6: # if i+1 in range(ln-5,ln+6):
          print(f"{i+1}: {line[:pn-1]}\x1b[31m{line[pn-1:]}\x1b[0m" if i+1 == ln else f"{i+1}: {line}")
    raise Exception(error_messages)
  else:
    destination = result.destination if result.ddl_target_table is None else result.ddl_target_table
    print(f"\x1b[92mDestination: {destination}\x1b[0m")
    print(f"\x1b[93m{result.dml_stats}\x1b[0m") if result.dml_stats else None
    return result.to_dataframe()
print(f"Created {q} at: {pd.Timestamp.now('Singapore').strftime('%Y-%m-%d %H:%M')}")