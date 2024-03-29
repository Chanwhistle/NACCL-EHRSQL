
import os
import openai
import json
from tqdm import tqdm
import re

class Model():
    def __init__(self, client, model_name):
        self.client = client
        self.model_name = model_name
        current_real_dir = os.getcwd()
        # current_real_dir = os.path.dirname(os.path.realpath(__file__))
        target_dir = os.path.join(current_real_dir, 'chatgpt_api_key.json')

        if os.path.isfile(target_dir):
            with open(target_dir, 'rb') as f:
                openai.api_key = json.load(f)['key']
        if not os.path.isfile(target_dir) or openai.api_key == "":
            raise Exception("Error: no API key file found.")

    def ask_chatgpt(self, prompt, temperature=0.0):
        response = self.client.chat.completions.create(
                    model=self.model_name,
                    temperature=temperature,
                    messages=prompt
                )
        return response.choices[0].message.content

    def generate(self, input_data):
        """
        Arguments:
            input_data: list of python dictionaries containing 'id' and 'input'
        Returns:
            labels: python dictionary containing sql prediction or 'null' values associated with ids
        """

        labels = {}

        for item in tqdm(input_data):
            try:
                answer = self.ask_chatgpt(item['input'])
            except:
                answer = "null"
            labels[item["id"]] = post_process(answer)
            labels[item["id"]] = answer
            print(answer)
        return labels
    
    def generate_single(self, input_data):
        labels = {}
        for item in input_data:
            answer = self.ask_chatgpt(item['input'])

        return answer
    

def post_process(answer):
    answer = answer.replace('\n', ' ')
    answer = re.sub('[ ]+', ' ', answer)
    return answer
