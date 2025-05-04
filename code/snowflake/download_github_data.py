import gzip
import json
import urllib.request


import urllib.request
import sys
import shutil

# URL of the file to download
# Replace with the actual URL
file_url = 'http://data.gharchive.org/2015-01-01-12.json.gz'

# Local path where you want to save the file
# Replace with your desired file name/path
local_filename = 'downloaded_file.ext'

print(f"Attempting to download {file_url} to {local_filename}...")
user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
headers = {'User-Agent': user_agent}
chunk_size = 8192  # 8 * 1024 bytes

#try:
# Download the file from the URL and save it locally
# --- Create a Request object with headers ---
req = urllib.request.Request(file_url, headers=headers)
# -------------------------------------------

# --- Open the URL using the Request object ---
with urllib.request.urlopen(req) as response:
    # Check if the request was successful (HTTP status code 200 OK)
     # Note: urlopen with a Request object will raise HTTPError on bad status codes (like 403)
     # so this check might seem redundant, but it's good practice if you handle errors differently.
    if response.getcode() == 200:
        # Open the local file in binary write mode ('wb')
        with open(local_filename, 'wb') as out_file:
            # Use shutil.copyfileobj for efficient chunked copying
            shutil.copyfileobj(response, out_file, length=chunk_size)
        print(f"Successfully downloaded {local_filename}")
    # This else block might not be reached if HTTPError is raised first
    else:
        print(f"Failed to download. Server returned status code: {response.getcode()}", file=sys.stderr)
