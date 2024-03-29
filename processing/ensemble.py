import json
import os
from tqdm import tqdm

def sort_by_rule(rule, file, id):
    answer_key = {}
    keys = [k for k in file[id][rule] if k.endswith(rule)]
    for key in keys:
        answer = file[id][rule][key]
        # import IPython; IPython.embed(colors='linuxa')
        if not answer in answer_key.keys():
            answer_key[answer] = [key]
        else:
            answer_key[answer].append(key)
    sorted_answer_key = sorted([(answer, key) for answer, key in answer_key.items()], key=lambda x: len(x[1]))[::-1]
    return sorted_answer_key


def majority_vote(file, threshold, nums, rule):
    threshold = nums * threshold
    ids = [k for k in file]
    result = {}
    for id in ids:
        sorted_answer_key = sort_by_rule(rule, file, id)
        sorted_answer_key_by_answer = sort_by_rule('answer', file, id)
        # import IPython; IPython.embed(colors='linux')
        if len(sorted_answer_key[0][1]) >= threshold:
            result[id] = file[id]['sql'][sorted_answer_key[0][1][0].split('_')[0]+ '_sql']
        else:
            if len(sorted_answer_key_by_answer[0][1]) == (1 * nums):
                if sorted_answer_key_by_answer[0][0] not in ["[]", "[['None']]"]:
                    result[id] = file[id]['sql'][sorted_answer_key[0][1][0].split('_')[0]+ '_sql']
                else:
                    result[id] = "null"
            else:
                result[id] = "null"
            
    return result
                



if __name__ == "__main__":
    result = {}
    ensemble_submission = {}

    prediction_dir = 'analysis/ensemble/'
    start = 'test_base_ver1_fold_0_temp_1.0'
    end = 'qsa_final.json'
    files = os.listdir(prediction_dir)
    files = [f for f in files if f.startswith(start) and f.endswith(end)]
    num_files = len(files)
    
    data = {i: json.load(open(os.path.join(prediction_dir, f))) for i, f in enumerate(files)}
    ids = [k for k in data[0].keys()]
    
    for id in tqdm(ids):     
        score = 0
        answer = {}
        sqls = {}

        for i, f in enumerate(files):
            finetuned = data[i]
            pred = finetuned[id]['answer']
            sql = finetuned[id]['sql']
            question = finetuned[id]['question']
            if i == 0:
                answer = {f'finetuned{i}_answer' : pred}
                sqls = {f'finetuned{i}_sql' : sql}
            else:
                answer.update({f'finetuned{i}_answer' : pred})
                sqls.update({f'finetuned{i}_sql' : sql})
        
        result[id] = {'question': question, 
                    'answer': answer, 
                    'sql': sqls}
        
    ids = [k for k in result.keys()]
    for id in ids:
        score =0
        for i in range(num_files):
            if (i+1) == num_files:
                continue
            for j in range(i+1, num_files):
                if result[id]['sql'][f'finetuned{i}_sql'] == result[id]['sql'][f'finetuned{j}_sql']:
                    score += 1
                else: 
                    pass
                
        result[id].update({'score': score})

    threshold = 0.6
    rule = 'sql'
    ensembled_file = majority_vote(result, threshold=threshold, nums=num_files, rule=rule)
    json.dump(result, open(os.path.join(prediction_dir, f'ensemble_{start}_{rule}_analysis.json'), 'w'), indent=4)
    json.dump(ensembled_file, open(os.path.join('test/ensemble', f'ensemble_{start}_thr_{threshold}_{rule}.json'), 'w'), indent=4)



