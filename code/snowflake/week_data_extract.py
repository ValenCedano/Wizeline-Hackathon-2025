import os
import gzip
import json # Added json import back
from typing import List, Dict, Any, Union # Keep Union for potential future use

def extract_and_save_text_as_json(folder_path: str, output_txt_path: str) -> bool:
    """
    Reads the decompressed text content from gzipped files (.gz) within a
    specified folder, concatenates them, and saves the result into a JSON file.

    Args:
        folder_path: The path to the folder containing the .gz files.
        output_json_path: The full path where the output JSON file should be saved.

    Returns:
        True if the JSON file was successfully created, False otherwise.
    """
    concatenated_text = ""
    separator = "\n" # Add a newline between content of different files

    if not os.path.isdir(folder_path):
        print(f"Error: Input folder not found at '{folder_path}'")
        return False # Indicate failure

    print(f"Scanning folder: '{folder_path}' for .gz files...")

    file_count = 0
    for filename in os.listdir(folder_path):
        if filename.endswith(".gz"):
            file_path = os.path.join(folder_path, filename)

            # Ensure it's actually a file
            if os.path.isfile(file_path):
                print(f"Processing file: '{filename}'")
                try:
                    # Open the gzipped file in text mode
                    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                        file_content = f.read()
                        # Only add separator if there's already content
                        if concatenated_text:
                            concatenated_text += separator
                        concatenated_text += file_content
                        file_count += 1
                        print(f"Successfully read text from '{filename}'")

                except gzip.BadGzipFile:
                    print(f"Error: '{filename}' is not a valid gzip file. Skipping.")
                except UnicodeDecodeError:
                     print(f"Error: Could not decode '{filename}' using UTF-8. Skipping.")
                except Exception as e:
                    print(f"An unexpected error occurred while processing '{filename}': {e}. Skipping.")

    if file_count == 0:
        print("No text content was extracted from .gz files. No JSON file created.")
        return False # Indicate failure as no data was processed

    # Prepare data for JSON output
    output_data = {"concatenated_text": concatenated_text}

    # Ensure the output directory exists
    output_dir = os.path.dirname(output_txt_path)
    if output_dir: # Check if output_dir is not empty (i.e., not just a filename)
        os.makedirs(output_dir, exist_ok=True)

    # Save the concatenated text to the specified JSON file
    try:
        with open(output_txt_path, 'w', encoding='utf-8') as outfile:
            outfile.write(concatenated_text) # Write the string directly
        print(f"\nFinished extraction. Concatenated text from {file_count} file(s).")
        print(f"Result saved to JSON file: '{output_txt_path}'")
        return True # Indicate success
    except IOError as e:
        print(f"Error: Could not write to output file '{output_txt_path}': {e}")
        return False # Indicate failure
    except Exception as e:
        print(f"An unexpected error occurred while saving the JSON file: {e}")
        return False # Indicate failure


# --- Example Usage ---
if __name__ == "__main__":
    # Create a dummy folder and some dummy gzipped files for testing
    dummy_folder = r"/home/alice-cely/HackData/3_days"
    output_file = "concatenated_result.json" # Put output in a subfolder

    extract_and_save_text_as_json(dummy_folder, output_file)