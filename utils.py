import os
import json
from dail_sql.mask_question_utils import *
from dail_sql.sql2skeleton_utils import *


def read_json(path):
    with open(path) as f:
        file = json.load(f)
    return file

def write_label(path, file):
    os.makedirs(os.path.split(path)[0], exist_ok=True)
    with open(path, 'w+') as f:
        json.dump(file, f, indent=4)


