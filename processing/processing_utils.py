import os
import re
import json
import pandas as pd
import difflib

# Function to clean and adjust the SQL query string
def clean_sql_query_directly(query):
    # Remove unnecessary characters like ``` and the word 'sql'
    replace_phrase = ['```sql ', '```', 'sql\n','SQL Query: ', 'sql ', 'SQL:']
    cleaned_query = query
    for phrase in replace_phrase:
        try:
            cleaned_query = cleaned_query.replace(phrase, '').stri[(0)]
        except:
            cleaned_query = cleaned_query
    try:
        cleaned_query = cleaned_query.replace('SELECT SELECT', 'SELECT')
    except:
        cleaned_query = cleaned_query
    # Ensure the query ends with a semicolon
    if not cleaned_query.endswith(';'):
        cleaned_query += ';'
    return cleaned_query

# Function to find and clean the SQL query within a larger text
def clean_sql_query_within_text(query):
    # Regular expression to find the SQL query block
    
    # import IPython; IPython.embed(colors='linux')
    sql_block_pattern = re.compile(r'```sql(.*?)```', re.DOTALL)
    match = sql_block_pattern.search(query)
    if match:
        # Extract and clean the SQL query from the match
        return clean_sql_query_directly(match.group(0))
    else:
        # Return None if no SQL block is found
        return None


def load_all_data(db_path, tables_name_file_name):
    db_schema = json.load(open(os.path.join(db_path, tables_name_file_name), 'r'))
    tables_name = db_schema[0]['table_names_original']
    total_data = {}
    for table in tables_name:
        data = pd.read_csv(os.path.join(db_path, f'{table}.csv'))
        total_data[table] = data
    return total_data, tables_name


def find_table_column(query, tables_name, table_data):
    table_columns = []
    for table in tables_name:
        data = table_data[table]
        columns = list(data.columns)
        # if query == 'diagnostic ultrasound of digestive system':
        #     import IPython; IPython.embed(colors='linux')
        for col in columns:
            if query in data[f'{col}'].values:
                table_columns.append((table, col))   
    
    return table_columns


def match_pattern(pattern, query):
    pattern_names = []
    match = re.findall(pattern, query)
    if match and len(match[0])==3:
        for m in match:
            extracted_value = m[2] 
            table_name = m[1].split('.')[0]
            if not (extracted_value in ['start of year', '-0 year']):
                if (m[0] == table_name):
                    pattern_names.append((extracted_value, m[0], m[1], m[2]))
    elif match and len(match[0])==2:
        for m in match:
            extracted_value = m[1] 
            table_name = m[0].split('.')[0]
            if not (extracted_value in ['start of year', '-0 year']):
                pattern_names.append((extracted_value, 'uppercase', m[0], m[1]))
    elif match and len(match[0])==4:
        for m in match:
            extracted_value = m[3] 
            table_name = m[2].split('.')[0]
            if not (extracted_value in ['start of year', '-0 year']):
                formatted_matches = (extracted_value, m[0] + " " + m[1], m[2], m[3])
                pattern_names.append(formatted_matches)
    return pattern_names


def find_pattern(query):
    pattern_names = []
    pattern1 = r"FROM\s+(\w+)\s+WHERE\s+([\w\.]+)\s*=\s*'([^']*)' "
    result = match_pattern(pattern1, query)
    if result != []: 
        pattern_names += result
    pattern2 = r"FROM (\w+).*?(\S+\.\w+) = '([^']+)' "
    result = match_pattern(pattern2, query)
    if result != []: 
        pattern_names += result
    if pattern_names == []:
        pattern3 = r"FROM\s+(\w+)\s+(\w+)\s+WHERE\s+(\w+\.\w+)\s*=\s*'([^']+)'"
        result = match_pattern(pattern3, query)
        if result != []:
            pattern_names += result
        elif result == []:
            pattern4 = r"(\b\w+\.\w+)\s*=\s*'([^']+)' "
            result = match_pattern(pattern4, query)
            if result != []:
                pattern_names += result
            else:
                pattern_names = "null"
    return pattern_names
    


def highlight_differences(original_query, replaced_query):
    # Splitting both queries into lists of words for a detailed comparison
    original_words = original_query.split()
    replaced_words = replaced_query.split()

    # Creating a sequence matcher to compare the two lists of words
    matcher = difflib.SequenceMatcher(None, original_words, replaced_words)

    # Preparing lists to hold words that have changed
    original_diffs = []
    replaced_diffs = []

    # Looping through the matching blocks and finding differences
    for opcode in matcher.get_opcodes():
        tag, i1, i2, j1, j2 = opcode
        if tag == 'replace':
            original_diffs.extend(original_words[i1:i2])
            replaced_diffs.extend(replaced_words[j1:j2])
        elif tag == 'delete':
            original_diffs.extend(original_words[i1:i2])
        elif tag == 'insert':
            replaced_diffs.extend(replaced_words[j1:j2])

    # Joining the different words back into strings for display
    original_diffs_str = ' '.join(original_diffs)
    replaced_diffs_str = ' '.join(replaced_diffs)

    return original_diffs_str, replaced_diffs_str



def match_name(table_data, tables_name, query, id, id_question):
    table_columns= []
    pattern_names = find_pattern(query)
    exist = None
    count = 0
    if pattern_names != "null": 
        for extracted_value, ori0, ori1, ori2 in pattern_names:
            if not ' ' in extracted_value:
                continue
            original_phrase = f"FROM {ori0} WHERE {ori1} = '{extracted_value}'"
            table_columns = find_table_column(extracted_value, tables_name, table_data)
            if table_columns != []:
                exist = False
                for table, col in table_columns:
                    replaced_phrase = f"WHERE {table}.{col} = '{extracted_value}'"
                    if replaced_phrase in query:
                        exist = True
                        break

                if exist == True:
                    yes =  False
                    for word in extracted_value.split(' '):
                        if word in id_question[id]:
                            yes = True
                    if (yes == False):
                        query = "null"

                    return query, table_columns, count
                elif exist == False:
                    replaced_phrase = f"FROM {table} WHERE {table}.{col} = '{extracted_value}'"
                    if len(set([table for table, col in table_columns])) >=2:
                        extracted_table_name = ori0
                        extracted_phrase = [(table, col) for table, col in table_columns if table == extracted_table_name]
                        if len(extracted_phrase) != 1: 
                            import IPython; IPython.embed(colors='linux')

                        replaced_phrase = f"FROM {extracted_phrase[0][0]} WHERE {extracted_phrase[0][0]}.{extracted_phrase[0][1]} = '{extracted_value}'"        
                    
                    replaced_query = query.replace(original_phrase, replaced_phrase)
                    if query != replaced_query:
                        pass

                    elif query == replaced_query: 
                        ori_split_start = original_phrase.split('WHERE')[0] 
                        ori_split_end = original_phrase.split('WHERE')[1]
                        rep_split_start = replaced_phrase.split('WHERE')[0]
                        rep_split_end = replaced_phrase.split('WHERE')[1]
                        try:
                            mid_line = query.split(ori_split_start)[1].split(ori_split_end)[0]
                        except:
                            mid_line = "null"
                        original_phrase = ori_split_start  + mid_line + ori_split_end
                        replaced_phrase = rep_split_start  + mid_line + rep_split_end
                        replaced_query = query.replace(original_phrase, replaced_phrase)

                    query = replaced_query

            if table_columns == []:
                query = "null"
            
    return query, table_columns, count


def jul_change(sql_query):
  
    # Replace julianday('now') with strftime('%J', '2100-12-31 23:59:00')
    transformed_query = sql_query.replace("julianday('now')", "strftime('%J', '2100-12-31 23:59:00')")

    # Replace julianday(variable) with strftime('%J', variable)
    # Use regex to find and replace all instances of julianday(variable)
    import re
    pattern = r"julianday\((.*?)\)"
    replacement = r"strftime('%J', \1)"
    transformed_query = re.sub(pattern, replacement, transformed_query)

    return transformed_query

def revise_pt_id_wrong(input_sql):

    pattern = r"subject_id ="
    match = re.search(pattern, input_sql)
    if match:
        pattern2 = r"subject_id = (\d{8})"
        match2 = re.search(pattern2, input_sql)
        if match2:
           return input_sql
        else:
            return "null"
    else:
        return input_sql
