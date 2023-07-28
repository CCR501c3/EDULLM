import json
import os, sys
import subprocess
import time

metadata_json = json.load(open("metadata_language.json"))

#token_splitter = RecursiveCharacterTextSplitter()
to_embed = []
running = []

def split_texts():
    checked = 0
    skipped = 0
    processed = 0
    
    all_tasks = []
    tasks=[]
    init_count = 0
    irrelevant = -1
    for name in metadata_json:
        elem = metadata_json[name]
        #if the elem has no language attribute or does and it is English
        if "language" not in elem or elem["language"] == ["English"]:
            #run external python script textify.py and wait for it to finish
            process = subprocess.Popen(["python", "textify.py", name])
            running.append(process)
            processed += 1
        else:
            skipped += 1
        checked += 1
        init_count += 1
        #load docs in batches of 10
        print(f"checked: {checked}, processed: {processed}, skipped: {skipped}")
        
        while len(running) >= 20:
            for p in running:
                if p.poll() is not None:
                    running.remove(p)
            time.sleep(0.25)
    return tasks




split_texts()