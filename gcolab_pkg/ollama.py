import os
import requests 
from icecream import ic

def setup_gpu(models = "llama3.1"):
    # @title get ollama 
    !curl -fsSL https://ollama.com/install.sh | sh
    !nvidia-smi && sudo apt-get update && sudo apt-get install -y pciutils
    
    os.environ['LD_LIBRARY_PATH'] = '/usr/lib64-nvidia'
def setup_ollama(models = "llama3.1"):
    !ollama serve > ollama.log 2>&1 & sleep 5
    !cat ollama.log
    !echo {models} | xargs -n1 -P3 ollama pull
    !ollama list
    !pip install -q ollama
    
    

def setup_bq():
    GITHUB_KEY = os.getenv('GITHUB_KEY')
    !pip install -q git+https://{GITHUB_KEY}@github.com/andrekamarudin/google_pkg.git 
    from google.colab import userdata
    import json
    from google_api.packages.gservice import ServiceKey
    from google_api.bigquery import BigQuery
    bq = BigQuery(
        project='fairprice-bigquery', 
        service_key=ServiceKey(**json.loads(userdata.get('DBDA_GOOGLE_APPLICATION_CREDENTIALS')))
    )
    %load_ext google.colab.data_table
    bq.q("Select 1")

def main():
    setup_gpu()
    setup_ollama()
    import ollama
    response = ollama.chat(
        model='llama3.1',
        messages=[
            {
                'role': 'user',
                'content': 'Explain the concept of a large language model in simple terms.',
            },
        ]
    )
    print(response['message']['content'])
    
if __name__ == "__main__":
    main()



def working_tool_call():
    def get_weather(city: str) -> str:
        """Return a simple weather string for a given city."""
        url = f"http://wttr.in/{city}"
        response = requests.get(url)
        if response.status_code == 200:
            return response.text
        else:
            return f"Error: {response.status_code}"

    messages = [
        {'role': 'user', 'content': 'What is the capital of China? I need the weather.'}
    ]

    response = ollama.chat(
        model='llama3.1',
        messages=messages,
        tools=[get_weather],
    )

    messages.append({
        'role': 'assistant', 
        'content': response['message']['content'] or str(response['message']['tool_calls']),
    })

    if response['message'].tool_calls:
        for call in response['message'].tool_calls:
            name = call.function.name
            args = call.function.arguments

            if name == 'get_weather':
                result = get_weather(**args)
                messages.append({
                    'role': 'tool',
                    'name': name,
                    'content': result
                })

            else:
                raise ValueError(f"Unknown tool requested: {name}")

        followup = ollama.chat(
            model='llama3.1',
            messages=messages,
            tools=[get_weather],
        )
        messages.append({
            'role': 'assistant',
            'content': followup['message']
        })

    # 6. Print out the entire thread
    for msg in messages:
        print(f"{msg['role'].upper()}: {msg['content']}")