import json
import openai
import argparse
from tqdm import tqdm

from llm import Model
from data.load_data import load_train_valid
from utils import *


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--gpt_v', required=True)
    parser.add_argument('--ver', required=True)
    args = parser.parse_args()
    
    
    api_key = ""
    client = openai.OpenAI(api_key = api_key)
    myModel = Model(client, args.gpt_v)
    
    
    ###file_name###
    prediction_file_name = f'pred/test_pick_correct_{args.gpt_v}_ensemble_ver_{args.ver}.json'
    prompt_file_name = 'data/table_description.json'
    ################
    
    masked_train_question_texts = []
    masked_train_ids = []
    masked_train_questions = json.load(open('data/masked_template_question.json', 'r'))

    
    for i, text in masked_train_questions.items():
        if 'template' not in i:
            masked_train_question_texts.append(text)
            masked_train_ids.append(i)
    
    prompt = json.load(open(prompt_file_name, 'r'))
    
    key = "verification_pick_correct"
    pick_correct_prompt = {'system_msg': prompt[key]['system_msg'], 
                      'user_msg': prompt[key]['user_msg']}
    
    key = "verification_null"
    null_prompt = {"system_msg": prompt[key]['system_msg'],
                   'user_msg': prompt[key]['user_msg']}
    

    ensemble_prediction = json.load(open('analysis/ensemble/ensemble_base_ver1.json', 'r'))
    prediction = json.load(open('pred/submission/test_base_ver1_prediction.json', 'r'))
    ids = [i for i in ensemble_prediction.keys()]
    
    train_data, train_label, valid_data, valid_label, test_data, test_label = load_train_valid(args.fold_num)
    train_questions = [item['question'] for item in train_data['data']]
    train_ids = [item['id'] for item in train_data['data']]
    train_id_questions = {item['id']: [item['question']] for item in train_data['data']}
    test_id_questions = {item['id']: item['question'] for item in test_data['data']}
    
    test_data = json.load(open('data/test_templatized_question.json'))
    test_reformulated_id_question = {item['id']: item['question'] for item in test_data['data']}
    pick_correct_input_data = []
    pick_null_input_data = []

    for i in tqdm(ids):
        question_id = i
        question_text = test_id_questions[i]
        sqls = ensemble_prediction[i]['sql']
        answers = ensemble_prediction[i]['answer']
        num_sql = len([k for k in sqls.keys()])
        prompt_sql = ""
        for j in range(num_sql):
            prompt_sql= prompt_sql + f"{j+1}. SQL: {sqls[f'finetuned{j}_sql']}\nAnswer: {answers[f'finetuned{j}_answer']}\n\n"

     
        pick_correct_conversation = [{'role': 'user', 'content': pick_correct_prompt['system_msg'] }]
        pick_correct_conversation.append({'role': 'user', 'content': pick_correct_prompt['user_msg'] + '\n\n' + 'question:'+ question_text + '\n\n'+prompt_sql})
        temp = {'input': pick_correct_conversation, 'id': question_id}
        pick_correct_input_data.append(temp)



    prediction = myModel.generate(pick_correct_input_data) 
    write_label(prediction_file_name, prediction)
    
    print(f"Completion to save the file. {prediction_file_name}")

    