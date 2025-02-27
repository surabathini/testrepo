import subprocess
import pandas as pd
import psycopg2

def fetch_tags():
    try:
        subprocess.run(['git', 'fetch', '--tags'], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error fetching tags: {e.stderr}")

def get_file_content(tag, file_path):
    try:
        result = subprocess.run(['git', 'show', f'{tag}:{file_path}'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, text=True)
        return result.stdout.splitlines()
    except subprocess.CalledProcessError as e:
        print(f"Error reading file for tag {tag}: {e.stderr}")
        return []

def extract_lines(lines):
    return [line for line in lines if line]

def read_git_files(tag1, tag2, file_path):
    lines_tag1 = get_file_content(tag1, file_path)
    lines_tag2 = get_file_content(tag2, file_path)

    array1 = extract_lines(lines_tag1)
    array2 = extract_lines(lines_tag2)

    return array1, array2

def merge_arrays(array1, array2):
    merged_array = array1.copy()
    for item in array2:
        if item not in merged_array:
            merged_array.append(item)
    return merged_array

def query_database():
    try:
        connection = psycopg2.connect(
            dbname="your_dbname",
            user="your_username",
            password="your_password",
            host="your_host",
            port="your_port"
        )
        cursor = connection.cursor()
        cursor.execute("SELECT column1 FROM table1")
        db_array = [row[0] for row in cursor.fetchall()]
        cursor.close()
        connection.close()
        return db_array
    except Exception as e:
        print(f"Error querying database: {e}")
        return []

def find_mismatch(merged_array, db_array):
    min_length = min(len(merged_array), len(db_array))
    for i in range(min_length):
        if merged_array[i] != db_array[i]:
            prior_row = merged_array[i-1] if i > 0 else None
            return prior_row, merged_array[i], db_array[i]
    if len(merged_array) != len(db_array):
        return merged_array[min_length-1] if min_length > 0 else None, merged_array[min_length:], db_array[min_length:]
    return None, None, None

def fetch_latest_content(branch_or_tag, file_path):
    try:
        subprocess.run(['git', 'fetch', 'origin', branch_or_tag], check=True)
        result = subprocess.run(['git', 'show', f'origin/{branch_or_tag}:{file_path}'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, text=True)
        return result.stdout.splitlines()
    except subprocess.CalledProcessError as e:
        print(f"Error fetching latest content for {branch_or_tag}: {e.stderr}")
        return []

def main():
    fetch_tags()

    array1 = fetch_latest_content('v1.0', 'path/to/your/file.txt')
    array2 = fetch_latest_content('v2.0', 'path/to/your/file.txt')
    print(array1)
    print(array2)

    merged_array = merge_arrays(array1, array2)
    print(merged_array)

    db_array = query_database()
    print(db_array)

    prior_row, merged_row, db_row = find_mismatch(merged_array, db_array)
    print(f"Prior row: {prior_row}")
    print(f"Merged array row: {merged_row}")
    print(f"Database array row: {db_row}")

    if prior_row is not None or merged_row is not None or db_row is not None:
        print(f"First mismatch found:")
        print(f"Prior row: {prior_row}")
        print(f"Merged array row: {merged_row}")
        print(f"Database array row: {db_row}")

        mismatch_index = merged_array.index(merged_row) if merged_row in merged_array else len(merged_array)

        for row in merged_array[mismatch_index:]:
            if row in db_array:
                print(f"Row found in database: {row}")
            else:
                print(f"Row needs to be added to database: {row}")
    else:
        print("No mismatches found.")

if __name__ == "__main__":
    main()
