from tqdm import tqdm
from processing_utils import *
from sql2answer import *
import argparse
import re


def clean_sql(data):
    # Iterate through each item and apply transformations
    for id, label in data.items():
        if label.startswith('SELECT'):
            cleaned_query = clean_sql_query_directly(label)
            data[id] = cleaned_query
        elif 'null' in label:
            cleaned_query = "null"
            data[id] = cleaned_query
        # elif 'None' in label:
        #     cleaned_query = "null"
        #     data[id] = cleaned_query
        else:
            cleaned_query = "null"
            data[id] = cleaned_query
            # import IPython; IPython.embed(colors='linux')
    return data

def convert_sql2answer(prediction, db_path):
    ids = [k for k in prediction.keys()]
    save_prediction = {}
    for i in ids:
        query_id = i
        sql = prediction[i]
        pred_dict = {query_id: sql}
        pred_results = execute_all(pred_dict, db_path, tag='pred')
        # print("Pred Answer: ", pred_results, "\n\n")
        save_prediction.update(pred_results)
    return save_prediction



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--nonevalue2null', action="store_true")
    args = parser.parse_args()
    
    
    reference_dir = "/ssd0/ehrsql/ehrsql-2024/data/mimic_iv"
    prediction_dir = f"/home/ehrsql/submission/pred/submission"
    # prediction_dir = f"/home/ehrsql/submission/test/ensemble"
    end = 'json'
    start = 'ensemble'
    # files = [f for f in os.listdir(prediction_dir) if f.endswith(end)]
    files = [f for f in os.listdir(prediction_dir) if f.startswith(start)]
    files = ['ensemble_ensemble_thr_0.8_sql.json', 'test_base_ver1_reformulated_highlighted_temp_0.7_ver_0_submission.json']
    for f in files: 
        # prediction_file_name = 'test_null_sql_temp_0.7_ver_4.json'
        # prediction_file_name = 'test_base_ver1_reformulated_highlighted_gpt4_temp_0.7_ver_0.json'
        prediction_file_name = f
        sqlite_file_name = 'mimic_iv.sqlite'
        tables_name_file_name =  'tables.json'
        
        retemple = json.load(open('data/test_templatized_highlighted_gpt4.json', 'r'))
        retemple = {item['id']: item['question'] for item in retemple['data']}
        prediction = json.load(open(os.path.join(prediction_dir, prediction_file_name), 'r'))
        prediction = clean_sql(prediction)
        assert (len([v for v in prediction.values() if v == "null"]) + len([v for v in prediction.values() if v.startswith('SELECT')])) == 1167
        
        db_path = os.path.join(reference_dir, 'mimic_iv.sqlite')
        if not os.path.exists(db_path): raise Exception('File does not exist: %s' % db_path)
        table_data, tables_name = load_all_data(reference_dir, tables_name_file_name)
        
        valid_data = json.load(open("/ssd0/ehrsql/ehrsql-2024/data/mimic_iv/valid/data.json", 'r'))
        valid_id_question = {item['id']: item['question'] for item in valid_data['data']}
        test_data = json.load(open("/ssd0/ehrsql/ehrsql-2024/data/mimic_iv/test/data.json", 'r'))
        test_id_question = {item['id']: item['question'] for item in test_data['data']}
    
        count_no_table_column = 0
        count_processed_sql = 0
        count_processed_table_value = 0
        count_processed_null = 0
        prediction_qsa = {}
        before_null_qsa = {}
        processed_prediction = {}
        ids = [id for id in prediction.keys()]
        for id in tqdm(ids):
            sql = prediction[id]
            sql = revise_pt_id_wrong(sql)
            sql = jul_change(sql)
            processed_sql = post_process_sql(sql)
            if sql != processed_sql: count_processed_sql += 1
            matched_sql, processed_table_columns, count = match_name(args, table_data, tables_name, processed_sql, id, test_id_question)
            count_no_table_column += count
            matched_sql = processed_sql
            if processed_sql != matched_sql: 
                if matched_sql != "null":
                    count_processed_table_value += 1
            processed_prediction[id] = processed_sql
        # import IPython; IPython.embed(colors='linux')
        num_workers = 1

        # prediction2answer = convert_sql2answer(processed_prediction, db_path, num_workers=num_workers)
        save_sql2answer=convert_sql2answer(processed_prediction, db_path)
        json.dump(save_sql2answer, open(os.path.join('pred/sql2answer', f'{prediction_file_name[:-5]}_sql2answer.json'), 'w'), indent=4)
        prediction2answer = json.load(open(os.path.join('pred/sql2answer', f'{prediction_file_name[:-5]}_sql2answer.json'), 'r'))
        for id in tqdm(ids):
            question = test_id_question[id]
            answer = prediction2answer[id]
            sql = processed_prediction[id]
            processed_sql = processed_prediction[id]
            before_null_sql = processed_prediction[id]
            processed_answer = answer
            # if (answer  == "[]") or (answer == "[['None']]"):
                # processed_answer = "null"
                # processed_sql = "null"
            if (processed_sql != 'null') and (answer == "null"):
                processed_sql = "null"
            if answer  == 'error_pred':
                processed_answer = "null"
                processed_sql = "null"
            if 'expected' in question:
                processed_answer = "null"
                processed_sql = "null"
            if ' height ' in question:
                processed_answer = "null"
                processed_sql = "null"
            
            if sql != processed_sql: count_processed_null += 1
            
            processed_prediction[id] = processed_sql
            prediction2answer[id] = processed_answer
            
            original_diffs, processed_diffs = highlight_differences(prediction[id], processed_prediction[id])
            difference = {
                            'original_sql': prediction[id],
                            'processed_sql': processed_prediction[id],
                            'original_diffs': original_diffs,
                            'processed_diffs': processed_diffs
            }
            prediction_qsa[id] = {
                        'question': test_id_question[id],
                        're_question': retemple[id],
                        'sql': processed_sql,
                        'answer': processed_answer,
                        'difference': difference
            }
            before_null_qsa[id] = {
                        'question': test_id_question[id],
                        'sql': before_null_sql,
                        'answer': answer,
            }

        print('count_no_table_column:', count_no_table_column)
        print('count_processed_sql:', count_processed_sql)
        print('count_processed_table_value:', count_processed_table_value)
        print('count_processed_null:', count_processed_null)
        
        json.dump(prediction_qsa, open(os.path.join('./analysis/ensemble', f'{prediction_file_name[:-5]}_qsa_final.json'), 'w'), indent=4)
        # json.dump(processed_prediction, open(os.path.join('./pred/submission', f'{prediction_file_name[:-5]}_submission_final.json'), 'w'), indent=4)
        # json.dump(before_null_qsa, open(os.path.join('./analysis', f'{prediction_file_name[:-5]}_before_null_final.json'), 'w'), indent=4)