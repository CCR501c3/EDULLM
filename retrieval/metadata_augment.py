import requests
import json
import os

# Set the base URL for the API
#base_url = 'https://api.ies.ed.gov/eric/?search=peerreviewed%3AT&format=json&rows=2000&fields=e_fulltextauth%3A1%2Clanguage%3Aenglish%2Csourceid%2Curl%2Cid'
base_url = 'https://api.ies.ed.gov/eric/?search=peerreviewed%3AT&format=json&rows=2000&fields=id%2Ce_fulltextauth%3A1%2Cpublicationdateyear%2Cauthor%2Cdescription%2Csourceid%2Csource%2Curl%2Cidentifierstest%2Cidentifierslaw%2Cidentifiersgeo%2Ceducationlevel%2Caudience%2Csource%2Curl%2Cinstitution%2Cisbn%2Cissn%2Clanguage'
# Set the maximum number of records to return per request
batch_size = 2000

# Set the starting record index
start_index = 0

# Initialize an empty list to store the results
results = []
download_count = 0
# Loop through the API results until we have all the records
directory = "../eric_gov"
metadata = {}
added = 0
skipped = 0

while True:
    # Make a request to the API
    response = requests.get(f'{base_url}&start={start_index}')

    # Parse the JSON response
    data = json.loads(response.text)
    docs = data['response']['docs']
    
    # Add the results to our list
    results.extend(docs)
    response_total = data['response']['numFound']
    print(f"files {download_count} downloaded so far. -- current offset: {100*start_index/response_total}%")
    for record in docs:
        if 'e_fulltextauth' in record:
            doc_id = record['id']
            file_path = os.path.join(directory, f"{doc_id}.pdf")
            if os.path.exists(file_path):
                added +=1
                metadata[doc_id] = record
            else: 
                skipped +=1
    print(f"{added} , {skipped} skipped")
    # If we've retrieved all the records, break out of the loop
    if start_index + batch_size >= response_total:
        break

    # Otherwise, update the starting index for the next request
    start_index += batch_size

with open("metadata_language.json", 'w') as outfile:
    # Save the JSON data to the file
    json.dump(metadata, outfile)