import json
from tqdm import tqdm 
from load_data import load_train_valid
from data_utils import *
        


def make_single_step_data(file_name, prompt, type):
    train_highlighted_question = json.load(open('train_highlighted_question.json', 'r'))
    test_highlighted_question = json.load(open('test_highlighted_question.json', 'r'))
    test_data = json.load(open('data/test_templatized_question.json', 'r'))
    if type == "train":
        data = train_data['data']
        highlighted_question = train_highlighted_question
    else:
        data = test_data['data']
        highlighted_question = test_highlighted_question
    with open(file_name, 'w') as f:
        for item in tqdm(data):
            input_data= {}
            question_id = item['id']
            question_text = item['question']
            question_text = change_normal_range(question_text)
            if type == "train":
                question_text = highlighted_question[question_id]
            elif type == "test":
                question_text = mask_question([question_text], '')[0]
            question_text = question_text[0].upper() + question_text[1:]
            # print(question_text)
            table_info = prompt['table_info']

            system_msg = prompt['system_msg2'] + '\n\nTables: \n' + table_info + '\n\nForeign keys:\n' + prompt['foreign_keys']
            conversation = [{"role": "user", "content": system_msg}]
            user_question_wrapper = lambda question: '\n\n' + f"""NLQ: \"{question}\"\nSQL: """
            conversation.append({"role": "user", "content": user_question_wrapper(question_text)})
            if type == 'train':
                label = train_label[question_id]
                conversation.append({"role": "assistant", "content": label})
                input_data['messages'] = conversation

                if label != "null":
                    json.dump(input_data, f)
                    f.write('\n')
            else:
                input_data['messages'] = conversation
                input_data['id'] = question_id
                json.dump(input_data, f)
                f.write('\n')


def make_two_step_data(file_name, test_prediction_file_name, prompt, type):
    test_data = json.load(open('data/test_templatized_question.json', 'r'))
    with open(file_name, 'w') as f:
        prediction = json.load(open(test_prediction_file_name, 'r'))
        for item in tqdm(test_data['data']):
            input_data= {}
            question_id = item['id']
            question_text = item['question']
            sql = prediction[question_id]
            
            question_text = change_normal_range(question_text)
            question_text = mask_question([question_text], '')[0]

            table_info = ""
            tables = find_table_name(tables_name, sql)
            for t in tables:
                table_info = table_info + prompt['table_info'][t]
            if tables == []:
                table_info = "null"
                tables = "null"
            system_msg = prompt['system_msg'] + '\n\nTables: \n' + table_info
            conversation = [{"role": "user", "content": system_msg}]
            user_question_wrapper = lambda question, tables: '\n\n' + f"""NLQ: \"{question}\"\nTables:{tables}\nSQL: """
            conversation.append({"role": "user", "content": user_question_wrapper(question_text, tables)})
            input_data['messages'] = conversation
            if type == "test": 
                input_data['id'] = question_id
            json.dump(input_data, f)
            f.write('\n')




db_schema_file_name =  f'/ssd0/ehrsql/ehrsql-2024/data/mimic_iv/tables.json'
db_schema = json.load(open(db_schema_file_name, 'r'))
tables_name = db_schema[0]['table_names_original']

prompt_file_name = '/home/ehrsql/submission/data/table_description.json'
prompt = json.load(open(prompt_file_name, 'r'))

key = "base_ver1"
base_prompt = {'system_msg': prompt[key]['system_msg'],
                            'user_msg': "NLQ: {question}\nSQL:",
                            'table_info': rewrite_table_info(prompt[key]['table_info'], 'original'),
                            'foreign_keys': prompt[key]['foreign_keys'],
                'system_msg2': prompt[key]['system_msg2']
}

key = "detailed_description"
twostep_prompt = {'system_msg': prompt[key]['system_msg'].replace('17 tables', 'tables'),
                            'user_msg': "NLQ: {question}\nTables: {tables}\nSQL:",
                            'table_info': rewrite_table_info(prompt[key]['table_info'], dict)
}


fold_num = "0"
train_data, train_label, valid_data, valid_label, test_data, test_label = load_train_valid(fold_num)

train_file_name = f'data/train_base_ver1.jsonl'
test_file_name = f'data/test_base_ver1.jsonl'
make_single_step_data(train_file_name, base_prompt, type = "train")
make_single_step_data(test_file_name, base_prompt, type="test")

train_file_name = f'data/train_twostepjsonl'
test_file_name = f'data/test_twostep.jsonl'
make_two_step_data(train_file_name, twostep_prompt, type = "train")
make_two_step_data(test_file_name, twostep_prompt, type = "test")