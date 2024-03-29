import os 
import json
import pandas as pd

def read_data(path):
    with open(path) as f:
        file = json.load(f)
    return file

def load_fold(path):
    fold = pd.read_csv(path)
    return fold

# Directory paths for database, results and scoring program
DB_ID = 'mimic_iv'
BASE_DATA_DIR = '/ssd0/ehrsql/ehrsql-2024/data/mimic_iv'
SCORE_PROGRAM_DIR = '/ssd0/ehrsql/ehrsql-2024/scoring_program/'
# File paths for the dataset and labelsa
TABLES_PATH = os.path.join('/ssd0/ehrsql/ehrsql-2024/', 'data', 'mimic_iv', 'tables.json')      
TRAIN_DATA_PATH = os.path.join(BASE_DATA_DIR, 'train', 'data.json')    
TRAIN_LABEL_PATH = os.path.join(BASE_DATA_DIR, 'train', 'label.json')
VALID_DATA_PATH = os.path.join(BASE_DATA_DIR, 'valid', 'data.json')   
TEST_DATA_PATH = os.path.join(BASE_DATA_DIR, 'test', 'data.json')
DB_PATH = os.path.join('/ssd0/ehrsql/ehrsql-2024/', 'data', 'mimic_iv.sqlite')


def load_train_valid(fold_num):
    train_data = read_data(TRAIN_DATA_PATH)
    train_label = read_data(TRAIN_LABEL_PATH)
    train_ids = [k for k in train_label.keys()]
    valid_data = read_data(VALID_DATA_PATH)
    test_data = read_data(TEST_DATA_PATH)
    
    if fold_num == "0":
        valid_label = {item['id']:"" for item in valid_data['data']},
        test_label = {item['id']:"" for item in test_data['data']}
        return train_data, train_label, valid_data, valid_label, test_data, test_label  

    else:
        fold_valid_info = load_fold(f'data/fold_dataset_{fold_num}.csv')
        fold_train_ids = list(set(train_ids) - set(fold_valid_info['id']))
        fold_train_data, fold_valid_data = {}, {}
        fold_train_data['data'] = [{'id': item['id'], 'question': item['question']} for item in train_data['data'] if item['id'] in fold_train_ids]
        fold_train_label = {id: train_label[id] for id in fold_train_ids}
        
        fold_valid_data['data'] = [{'id': i, 'question': q} for i, q in zip(fold_valid_info['id'], fold_valid_info['question'])]
        fold_valid_label = {id: label for id, label in zip(fold_valid_info['id'], fold_valid_info['label'])}

        test_label = {item['id']:"" for item in test_data['data']}
        return fold_train_data, fold_train_label, fold_valid_data, fold_valid_label, test_data, test_label