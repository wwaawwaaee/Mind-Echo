import os

def process_files_in_directory(directory):
    for filename in os.listdir(directory):
        if filename.endswith(".txt"):
            process_text_file(os.path.join(directory, filename))

def process_text_file(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
        # Perform text processing here