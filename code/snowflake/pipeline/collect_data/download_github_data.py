import gzip
import json
import urllib.request
import sys
import shutil

# URL of the file to download
file_url = 'http://data.gharchive.org/2015-01-01-12.json.gz'

# Local path to save the downloaded file
local_filename = 'downloaded_file.ext'

print(f"Downloading {file_url} to {local_filename}...")
user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
headers = {'User-Agent': user_agent}
chunk_size = 8192

req = urllib.request.Request(file_url, headers=headers)

try:
    with urllib.request.urlopen(req) as response, open(local_filename, 'wb') as out_file:
        if response.getcode() == 200:
            shutil.copyfileobj(response, out_file, length=chunk_size)
            print(f"Successfully downloaded {local_filename}")
        else:
            print(f"Download failed. Status code: {response.getcode()}", file=sys.stderr)
except urllib.error.URLError as e:
    print(f"Error downloading {file_url}: {e}", file=sys.stderr)
except Exception as e:
    print(f"An unexpected error occurred: {e}", file=sys.stderr)
