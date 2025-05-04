import os
import gzip
import json
from typing import List, Dict, Any, Union

def extract_and_save_text_as_json(folder_path: str, output_json_path: str) -> bool:
    """
    Reads text from gzipped files in a folder, concatenates, and saves to a JSON file.

    :param folder_path: Path to the folder containing .gz files.
    :type folder_path: str
    :param output_json_path: Full path for the output JSON file.
    :type output_json_path: str
    :raises FileNotFoundError: If `folder_path` doesn't exist.
    :raises IOError: If writing to `output_json_path` fails.
    :raises gzip.BadGzipFile: If a '.gz' file is invalid.
    :raises UnicodeDecodeError: If UTF-8 decoding fails.
    :returns: True if successful, False otherwise.
    :rtype: bool
    """
    concatenated_text = ""
    separator = "\n"

    if not os.path.isdir(folder_path):
        print(f"Error: Folder not found at '{folder_path}'")
        return False

    print(f"Scanning '{folder_path}' for .gz files...")

    file_count = 0
    for filename in os.listdir(folder_path):
        if filename.endswith(".gz"):
            file_path = os.path.join(folder_path, filename)
            if os.path.isfile(file_path):
                print(f"Processing '{filename}'")
                try:
                    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                        file_content = f.read()
                        if concatenated_text:
                            concatenated_text += separator
                        concatenated_text += file_content
                        file_count += 1
                        print(f"Read text from '{filename}'")
                except gzip.BadGzipFile:
                    print(f"Error: '{filename}' is not a valid gzip file. Skipping.")
                except UnicodeDecodeError:
                    print(f"Error: Could not decode '{filename}' with UTF-8. Skipping.")
                except Exception as e:
                    print(f"Error processing '{filename}': {e}. Skipping.")

    if file_count == 0:
        print("No text extracted. No JSON file created.")
        return False

    output_data = {"concatenated_text": concatenated_text}
    output_dir = os.path.dirname(output_json_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    try:
        with open(output_json_path, 'w', encoding='utf-8') as outfile:
            outfile.write(concatenated_text)
        print(f"\nExtracted from {file_count} file(s).")
        print(f"Result saved to: '{output_json_path}'")
        return True
    except IOError as e:
        print(f"Error writing to '{output_json_path}': {e}")
        return False
    except Exception as e:
        print(f"Error saving JSON file: {e}")
        return False


# --- Example Usage ---
if __name__ == "__main__":
    dummy_folder = r"/home/alice-cely/HackData/3_days"
    output_file = "concatenated_result.txt"

    extract_and_save_text_as_json(dummy_folder, output_file)
