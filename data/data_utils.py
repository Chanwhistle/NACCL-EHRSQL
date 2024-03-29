import os
import pandas as pd
import re

# inform normal range
def change_normal_range(question):
    __precomputed_dict = {
                    'temperature': (35.5, 38.1),
                    'o2': (95.0, 100.0),
                    'heart rate': (60.0, 100.0),
                    'respiratory rate': (12.0, 18.0),
                    'systolic blood pressure': (90.0, 120.0),
                    'blood pressure diastolic':(60.0, 90.0),
                    'mean bp': (60.0, 110.0)
                                }
    keys = [k for k in __precomputed_dict.keys()]
    if 'normal' in question:
        for key in keys:
            if key in question:
                normal_range = __precomputed_dict[key]
                start, end = normal_range
                additional_info = f" Note that {key} normal range is {start} to {end}."
                question = question + additional_info
    return question


# mask table specific information
def search_table_value(db_info, question, value_tag=None):
    used_value = []
    tables = [table for table in db_info.keys()]
    for table in tables:
        tablecolumns = ['long_title', 'label', 'drug']
        for col in tablecolumns:
            if col not in db_info[table].keys():
                continue
            values = db_info[table][col]
            values = sorted([(v, len(v)) for v in values if (pd.isna(v) ==False) and (len(v) >= 10) and (' ' in v)], key= lambda x: x[1])[::-1]
            values = [v[0] for v in values]
            # import IPython; IPython.embed(colors='linux')
            for v in values:
                if "'" in question:
                    continue
                if (f'{v}' in question.lower()):
                    used = False
                    for value in used_value:
                        if v in value:
                            used = True
                    if used == True:
                        continue
                    else:
                        used_value.append(v)
                    if value_tag == '<unk>':
                        question = question.lower().replace(f'{v}', f'{value_tag}')
                    else:
                        question = question.lower().replace(f'{v}', f"'{v}'")
    return question


def mask_question_with_schema_linking(db_info, question, value_tag):
    masked_question = search_table_value(db_info, question, value_tag)
    return masked_question


def construct_table_values(table, columns=None):
    tablecol_values = {}
    columns = list(table.columns)
    for col in columns:
        if 'time' not in col:
            unique_values = list(table[col].unique())
            if check_value(unique_values) == True:            
                tablecol_values[col] = unique_values
        
    return tablecol_values    

def make_db_info():
    dir_path = '/ssd0/ehrsql/ehrsql-2024/data/mimic_iv'
    files = os.listdir(dir_path)
    files = [f for f in files if f.endswith('.csv')]
    # files = ['d_icd_diagnoses.csv', 'd_icd_procedures.csv']
    db_info = {}
    for f in files:
        table = pd.read_csv(os.path.join(dir_path, f))
        table_name = f[:-4]
        tablecol_values = construct_table_values(table)
        db_info[table_name] = tablecol_values
    return db_info


def mask_question(questions, value_tag):
    db_info = make_db_info()
    masked_questions = []
    for question_text in questions:
        masked_questions.append(mask_question_with_schema_linking(db_info, question_text, value_tag=value_tag))
    return masked_questions


def extract_strings(text):
    # Use regular expression to match only strings (no numbers)
    try:
        strings_only = re.findall('[^\d\s]+', text)
    except:
        text = text.decode('utf-8') 
    return strings_only
    
    
def check_value(values):
    for v in values:
        if (pd.isna(v) == False) and isinstance(v, str):
            if extract_strings(v) != []:
                return True
            else:
                return False


def find_table_name(tables_name, pred_table_name):
    tables =[]
    for t in tables_name:
        if t in pred_table_name:
            tables.append(t)
    return tables




def rewrite_table_info(table_info, format=str):
    information = ""
    information_dict = {}
    if format == 'original':
        return table_info
    for i, info in enumerate(table_info):
        table_name_desc, table_schema = info.split('\n')[0], '\n'.join(info.split('\n')[1:])
        table_name, table_desc = table_name_desc.split(':')
        information = information + f'{i}. {table_name}: {table_desc}\nSchema: {table_schema}\n\n'
        information_dict[table_name] = f'{i}. {table_name}: {table_desc}\nSchema: {table_schema}\n\n'

    if format == str:
        return information
    elif format == dict:
        return information_dict
    else:
        raise KeyError


    