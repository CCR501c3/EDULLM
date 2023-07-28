import nest_asyncio
nest_asyncio.apply()

import asyncio
import aiohttp
import json
import requests
import os

# Set the base URL for the API
base_url = 'https://api.ies.ed.gov/eric/?search=peerreviewed%3AT&format=json&rows=2000&fields=id%2Ce_fulltextauth%3A1%2Cpublicationdateyear%2Cauthor%2Cdescription%2Csourceid%2Csource%2Curl%2Cidentifierstest%2Cidentifierslaw%2Cidentifiersgeo%2Ceducationlevel%2Caudience%2Csource%2Curl%2Cinstitution%2Cisbn%2Cissn'

# Set the maximum number of records to return per request
batch_size = 2000

# Set the starting record index
start_index = 0

# Initialize an empty list to store the results
results = []
download_count = 61355
successful_downloads = 0

# Create a directory to store the downloaded PDFs
output_path = '../eric_gov'
os.makedirs(output_path, exist_ok=True)
metadata = {}
added = 0
skipped = 0

async def download_pdf(session, url, record):
    try:
        async with session.get(url) as response:
            if response.ok is True:
                filename = url.split('/')[-1]
                filepath = os.path.join(output_path, filename)
                content = await response.read()
                if len(content) > 0:
                    print(f"downloaded: {filename}")
                    with open(filepath, 'wb') as file:
                        file.write(content)
                        metadata[record['id']] = record
                    return True
            #else:
            #    print(f"Error: {response.status} occurred while downloading {url}")
    except Exception as e:
        print(f"Error occurred while downloading {url}: {e}")
    return False

async def process_records(records):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for record in records:
            if 'e_fulltextauth' in record:
                doc_id = record['id']
                url = f'https://files.eric.ed.gov/fulltext/{doc_id}.pdf'
                tasks.append(download_pdf(session, url, record))
        results = await asyncio.gather(*tasks)
        return results

# Loop through the API results until we have all the records
while True:
    # Make a request to the API
    response = requests.get(f'{base_url}&start={start_index}')

    # Parse the JSON response
    data = json.loads(response.text)
    docs = data['response']['docs']

    # Add the results to our list
    results.extend(docs)
    response_total = data['response']['numFound']
    print(f"Files {download_count} downloaded so far. -- current offset: {100*start_index/response_total}%")

    # Create a list of records to process asynchronously
    records_to_process = [record for record in docs if 'e_fulltextauth' in record]

    # Download the PDFs asynchronously
    loop = asyncio.get_event_loop()
    download_results = loop.run_until_complete(process_records(records_to_process))

    download_count += len(records_to_process)
    successful_downloads += sum(download_results)

    # If we've retrieved all the records, break out of the loop
    if start_index + batch_size >= response_total:
        break

    # Otherwise, update the starting index for the next request
    start_index += batch_size

print(f"All PDFs downloaded. Successful downloads: {successful_downloads}/{download_count}")
with open("metadata2.json", 'w') as outfile:
    # Save the JSON data to the file
    json.dump(metadata, outfile)