import os
import re
import time

def extract_qna_sections_from_txt(txt_path):
    with open(txt_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    norm_lines = [line.strip().lower() for line in lines]

    start_idx = 0
    end_idx = None
    found_qna = False

    for i in range(len(norm_lines) - 1):
        if (norm_lines[i].startswith('=') or norm_lines[i].startswith('-')) and norm_lines[i + 1] == 'questions and answers':
            start_idx = i + 2
            found_qna = True
            break

    for i in range(start_idx + 1, len(norm_lines) - 1):
        if norm_lines[i] == 'definitions' and (norm_lines[i - 1].startswith('=') or norm_lines[i - 1].startswith('-')):
            end_idx = i - 1
            break

    qna_lines = lines[start_idx:end_idx] if end_idx is not None else lines[start_idx:]

    cleaned_lines = []
    i = 0
    while i < len(qna_lines) - 2:
        l1 = qna_lines[i].strip()
        l2 = qna_lines[i + 1].strip()
        l3 = qna_lines[i + 2].strip()
        if re.match(r'^[=-]{5,}$', l1) and re.match(r'^[=-]{5,}$', l3) and not re.match(r'^[=-]{5,}$', l2):
            i += 3
        else:
            cleaned_lines.append(qna_lines[i])
            i += 1

    while i < len(qna_lines):
        cleaned_lines.append(qna_lines[i])
        i += 1

    filtered_lines = [line.strip() for line in cleaned_lines if not re.match(r'^[=-]{5,}$', line.strip())]
    flat_text = ' '.join(filtered_lines)
    return flat_text


def extract_from_folder(folder_path, merged_output_path):
    total_files = len([f for f in os.listdir(folder_path) if f.endswith(".txt")])
    processed = 0
    start_time = time.time()

    with open(merged_output_path, 'w', encoding='utf-8') as merged_file:
        for filename in os.listdir(folder_path):
            if filename.endswith(".txt"):
                txt_path = os.path.join(folder_path, filename)
                extracted_qna = extract_qna_sections_from_txt(txt_path)
                merged_file.write(extracted_qna + "\n")
                processed += 1
                if processed % 1000 == 0:
                    elapsed = time.time() - start_time
                    print(f"  Processed {processed} / {total_files} files. Time: {elapsed:.1f}s")


def process_all_countries(base_path, output_path, selected_countries=None):
    all_folders = [f for f in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, f))]
    if selected_countries:
        all_folders = [f for f in all_folders if f in selected_countries]

    for country in sorted(all_folders):
        input_folder = os.path.join(base_path, country)
        output_file = os.path.join(output_path, f"merged_qna_{country}.txt")
        print(f"\n🔍 Processing country: {country}")
        extract_from_folder(input_folder, output_file)


if __name__ == '__main__':
    # Base path where each country folder is stored
    ec_base = r'J:\Saeed Work\May 20\data\country_data'

    # Example: Run for all countries
    #process_all_countries(base_path=ec_base, output_path=ec_base)

    # Or: Run only for selected countries
    # selected = ["Canada", "Mexico", "Iran"]

    #all_folders = [f for f in os.listdir(ec_base) if os.path.isdir(os.path.join(ec_base, f))]
    #selected = [f for f in all_folders if f != "USA"]
    selected = ["USA"]

    process_all_countries(base_path=ec_base, output_path=ec_base, selected_countries=selected)
