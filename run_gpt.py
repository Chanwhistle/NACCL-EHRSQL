import json
import openai
import argparse
from tqdm import tqdm

from llm import Model
from utils import *
from data.load_data import *
from data.search_example import *
from data.data_utils import *
from processing.sql2answer import *



def make_conversation(prompt, examples, question, question_id, table):
    data = {}
    if table != "":
        table_info = ""
        for name in table:
            table_info = table_info + prompt['table_info'][name]
        
        conversation = [{'role': 'system', 'content': prompt['system_msg']+
                                                            "\n\n"+
                                                            table_info+
                                                            "\n\n"+
                                                            examples}]
        conversation.append({'role': 'user', 'content': prompt['user_msg'].replace('{question}', question).replace('{tables}', str(table))})
    elif table == "":
        conversation = [{'role': 'system', 'content': prompt['system_msg']+
                                                            "\n\n"+
                                                            prompt['table_info']+
                                                            "\n\n"+
                                                            examples}]
        conversation.append({'role': 'user', 'content': prompt['user_msg'].replace('{question}', question)})
    data = {'input': conversation, 'id': question_id}
    return data



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--gpt_v', required=True)
    parser.add_argument('--n_shot', default=3, type =int)
    parser.add_argument('--fold_num', type=str)
    parser.add_argument('--masked', action = 'store_true')
    parser.add_argument('--step', choices=['table_selection', 'sql_generation', 'end_to_end', 'single_step', 'templatize'])
    args = parser.parse_args()
    
  
    api_key = ""
    client = openai.OpenAI(api_key = api_key)
    myModel = Model(client, args.gpt_v)
    
    
    ###file_name###
    table_label_file_name = ''
    neighbor_question_ids_file_name = ''
    masked_questions_file_name = ''
    highlighted_question_file_name = ''
    db_schema_file_name =  ''
    prompt_file_name = ''
    name = ''
    sql_generation_input_data_file_name = ''
    prediction_file_name = f'/home/ehrsql/submission/pred/test_fewshot{args.n_shot}_base_ver1_maksed_{str(args.masked).lower()}_{args.gpt_v}_{name}_prediction.json'
    ################

    highlighted_id_question = json.load(open(highlighted_question_file_name, 'r'))
    table_label = json.load(open(table_label_file_name, 'r'))
    db_schema = json.load(open(db_schema_file_name, 'r'))
    tables_name = db_schema[0]['table_names_original']
    
    masked_train_question_texts = []
    masked_train_ids = []
    train_masked_questions = json.load(open('data/masked_template_question.json', 'r'))
    train_highligted_questions = json.load(open('train_highlighted_question.json', 'r'))
    train_highlighted_id_question = {item['id']: item['question'] for item in train_highligted_questions}
    test_masked_id_question = json.load(open('test_masked_question_p.json', 'r'))
    test_highglighted_id_question = json.load(open('test_highlighted_question.json', 'r'))
    
    prompt = json.load(open(prompt_file_name, 'r'))

    key = "schema_natural_language"
    table_selection_prompt = {'system_msg': prompt[key]['system_msg'],
                                'user_msg': "NLQ: {question}\nTables:", 
                                'table_info': rewrite_table_info(prompt[key]['table_info'], str)
    }
    key = "detailed_description"
    sql_generation_prompt = {'system_msg': prompt[key]['system_msg'].replace('17 tables', 'tables'),
                                'user_msg': "NLQ: {question}\nTables: {tables}\nSQL:",
                                'table_info': rewrite_table_info(prompt[key]['table_info'], dict)
    }
    
    key = "base_ver1"
    base_prompt = {'system_msg': prompt[key]['system_msg'] ,
                                'user_msg': "NLQ: {question}\nTables: {tables} \nSQL:",
                                'table_info': rewrite_table_info(prompt[key]['table_info'], 'original'),
                                'foreign_keys': prompt[key]['foreign_keys']
    }

    key = "template"
    rewrite_prompt = {'system_msg': prompt[key]['system_msg'], 
                      'user_msg': 'question: {question}'}
    
    single_step_prompt = base_prompt
    single_step_prompt['user_msg'] = "NLQ: {question}\nSQL:"
    table_selection_prompt = ""
    


    train_data, train_label, valid_data, valid_label, test_data, test_label = load_train_valid(args.fold_num)
    train_questions = [item['question'] for item in train_data['data']]
    train_ids = [item['id'] for item in train_data['data']]
    train_id_questions = {item['id']: [item['question']] for item in train_data['data']}
    
    test_data = json.load(open('data/test_templatized_data.json'))
    sql_generation_input_data = []
    single_step_input_data = []
    neighbor_question_ids = {}
    rewrite_as_template_input_data =[]



    if args.step != "sql_generation":
        for data in tqdm(test_data['data']):
            question_id = data['id']
            question_text = data['question']
            neighbor_question_ids[question_id] = []

            masked_question = test_masked_id_question[question_id] 
            highlighted_question = test_highglighted_id_question[question_id]     
            
            table_selection_examples = ""
            sql_generation_examples = ""
            single_step_examples = ""
            if args.n_shot > 0:
                if os.path.exists(neighbor_question_ids_file_name):
                    neighbor_question_ids = json.load(open(neighbor_question_ids_file_name, 'r'))
                    neighbor_examples = []
                    for q_id in neighbor_question_ids[question_id][:args.n_shot]:
                        neighbor_examples.append((train_highlighted_id_question[q_id], q_id, ''))
                else:
                    if args.masked == True:
                        neighbor_examples = get_example(masked_train_question_texts, masked_train_ids, masked_question, args.n_shot)
                    else:
                        neighbor_examples = get_example(train_questions, train_ids, question_text, args.n_shot)
                

                table_selection_examples = "Examples:\n"
                sql_generation_examples = "Examples:\n"
                single_step_examples = "Examples:\n"
                for n, (neighbor_question, neighbor_id, _) in enumerate(neighbor_examples):
                    neighbor_question_ids[question_id].append(neighbor_id)
                    neighbor_question = train_highlighted_id_question[neighbor_id]
                    neighbor_sql_label = train_label[neighbor_id]
                    neighbor_table_label = table_label[neighbor_id]

        
                    table_selection_example_wrapper = f"\n\nNLQ: {neighbor_question}\nTables: {neighbor_table_label}"
                    sql_generation_example_wrapper = f"\n\nNLQ: {neighbor_question}\nTables: {neighbor_table_label}\nSQL: {neighbor_sql_label}"
                    single_step_example_wrapper = f"\n\nNLQ: {neighbor_question}\nSQL: {neighbor_sql_label}"
                    rewrite_examples_wrapper = f'{n+1}. id: "{neighbor_id}"\nquestion: "{neighbor_question}"\n'

                    
                    table_selection_examples = table_selection_examples + table_selection_example_wrapper
                    sql_generation_examples = sql_generation_examples + sql_generation_example_wrapper
                    single_step_examples = single_step_examples + single_step_example_wrapper
                    rewrite_examples = rewrite_examples + rewrite_examples_wrapper

                table_selection_conversation = make_conversation(table_selection_prompt, table_selection_examples, highlighted_question, question_id, table ='')
                
            
            if (args.step == "table_selection") or (args.step == "end_to_end"):
                pred_table = myModel.generate_single([table_selection_conversation])
                pred_table = find_table_name(tables_name, pred_table)
                sql_generation_conversation = make_conversation(sql_generation_prompt, sql_generation_examples, highlighted_question, question_id, table=pred_table)
                sql_generation_input_data.append(sql_generation_conversation)


            if args.step == "single_step":
                single_step_conversation = [{'role': 'user', 'content': single_step_prompt['system_msg'] + '\n\nTables:\n' + single_step_prompt['table_info']+ 
                                                                        '\n\nforeign_keys:\n' +single_step_prompt['foreign_keys'] +
                                                                        '\n\n' + single_step_examples}]
                single_step_conversation.append({'role': 'user', 'content': single_step_prompt['user_msg'].replace('{question}', highlighted_question)})
                temp = {'input': single_step_conversation, 'id': question_id}
                single_step_input_data.append(temp)

            
            if args.step == "templatize":
                rewrite_as_template_conversation = [{'role': 'user', 'content': rewrite_prompt['system_msg'] }]
                rewrite_as_template_conversation.append({'role': 'user', 'content': rewrite_examples})
                temp = {'input': rewrite_as_template_conversation, 'id': question_id}
                rewrite_as_template_input_data.append(temp)

    
 
    if (args.step == "table_selection") or (args.step == "end_to_end"):
        write_label(sql_generation_input_data_file_name, sql_generation_input_data)
    
    if (args.step == "sql_generation") or (args.step == "end_to_end"):
        sql_generation_input_data = json.load(open(sql_generation_input_data_file_name, 'r'))
        prediction = myModel.generate(sql_generation_input_data)
        write_label(prediction_file_name, prediction)
        
    elif args.step == "single_step":
        write_label(f'data/test_reformulated_masked_neighbor_questions_{args.n_shot}.json',  neighbor_question_ids)
        print("INFERENCE>>>")
        prediction = myModel.generate(single_step_input_data)
        write_label(prediction_file_name, prediction)

    if args.step == "templatize":
        write_label(f'data/train_templatize_{args.n_shot}_input_data.json',  rewrite_as_template_input_data)
        prediction = myModel.generate(rewrite_as_template_input_data)
        write_label(f'pred/train_templatized_question_{args.gpt_v}_n_shot_{args.n_shot}.json', prediction)

    print(f"Completion to save the file. {prediction_file_name}")

    
