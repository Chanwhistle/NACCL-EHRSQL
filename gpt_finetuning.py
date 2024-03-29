import openai
from openai import OpenAI
import argparse
import json
from tqdm import tqdm

parser = argparse.ArgumentParser()
parser.add_argument('--task', choices = ["upload", "finetune", "generate", "check"])
parser.add_argument('--model_name', type=str, required=True)
parser.add_argument('--fold_num', type=str)
parser.add_argument('--num', required=True)
parser.add_argument('--temperature', type=float, default=0.7)
args = parser.parse_args()

api_key = ""
file_path = "train_file"

if args.task == "upload":
    def upload(file_path):
        client = OpenAI(api_key = api_key )
        try:
            response = client.files.create(
                file=open(file_path, "rb"),
                purpose="fine-tune"
                )
            print(f"File uploaded successfully.")
            return response
        except Exception as e:
            print(f"An error occurred: {e}")
    # Call the function with the path to your .jsonl file
    file_id = upload(file_path)
    print(file_id)


if args.task == "finetune":
    client = OpenAI(api_key = api_key )
    def start_fine_tuning(file_id):
        try:
            # Start a fine-tuning job
            response = openai.FineTune.create(
                training_file=file_id,
                model="gpt-3.5-turbo",
                hyperparameters = {'n_epochs':1},
            )
            print(f"Fine-tuning job started successfully. Job ID: {response['id']}")
            return response['id']
        except Exception as e:
            print(f"An error occurred: {e}")
        
    def fine_tuning(file_id):
        client.fine_tuning.jobs.create(
            training_file=file_id ,
            model="gpt-3.5-turbo"
            )
        
    fine_tuning(file_id)


  
if args.task == "generate":
    print(args.model_name)
    test_name = f"data/test_base.jsonl"
    generated_sql = {}
    client = OpenAI(api_key = api_key)
    with open(test_name) as f:
        lines = f.readlines()
        for line in tqdm(lines):
            message = json.loads(line)['messages']
            q_id = json.loads(line)['id']
            completion = client.chat.completions.create(
                model= args.model_name,
                messages =message,
                temperature =args.temperature,
            )   
            result = completion.choices[0].message.content.strip()
            generated_sql.update({q_id: result})
    
    json.dump(generated_sql, open(f'test/finetuned_gpt/test_base_ver1_only_sql_temp_{args.temperature}_ver_{args.num}.json', 'w'), indent=4)
    

if args.task == "check":
    import IPython; IPython.embed(colors='linux')           


