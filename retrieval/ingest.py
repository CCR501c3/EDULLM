import json
import os, sys
import asyncio
from langchain.embeddings import HuggingFaceInstructEmbeddings
from langchain.indexes import VectorstoreIndexCreator
from langchain.vectorstores import Chroma
from langchain.document_loaders import PyPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter

#takes any elements in the metadata dict which are an array and converts them to a string
def textify(metadata):
    for key in metadata:
        if isinstance(metadata[key], list):
            metadata[key] = " ".join(metadata[key])
    return metadata



def split_text(vectordb, elem, checked, processed, skipped, percent):
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=300, chunk_overlap=0)
    #loader = PyPDFLoader(f"{pdf_directory}/{elem['id']}.pdf")
    loaddir = f"{json_directory}/{elem['id']}.json"
    print(f"loading: {loaddir}, {percent}%")
    with open(loaddir) as loaddir:
        chunks = json.load(loaddir)
    print(f"done loading: {loaddir}, percent: {percent}%")
    prevsource = ""
    chunknum = 0
    elem = textify(elem)
    metadatas = []
    texts = []
    for chunk in chunks['pages']:
        md= elem.copy()
        md["chunknum"] = chunknum
        metadatas.append(md)
        texts.append(chunk)
        chunknum += 1
    print(f"checked: {checked}, processed: {processed}, skipped: {skipped} percent: {percent}")
    if len(texts) > 0:    
        vectordb.add_texts(texts, metadatas)

metadata_json = json.load(open("metadata_language.json"))
json_directory = '../eric_gov_textified'

#token_splitter = RecursiveCharacterTextSplitter()
to_embed = []

def split_texts(vectordb: Chroma, start, amt):
    checked = 0
    skipped = 0
    processed = 0
    
    all_tasks = []
    tasks=[]
    init_count = 0
    startat = 249#len(metadata_json)*(start/100)
    endat = len(metadata_json)*((start+amt)/100)
    print(f"startat: {startat}, endat: {endat}")
    irrelevant = -1
    for name in metadata_json:
        irrelevant += 1
        if irrelevant < startat:
            continue
        if irrelevant > endat:
            break
        percent = round(100*processed/len(metadata_json), 4)
        elem = metadata_json[name]
        #if the elem has no language attribute or does and it is English
        if "language" not in elem or elem["language"] == ["English"]:
            task = split_text(vectordb, elem, checked, processed,skipped, percent)
            tasks.append(task)
            processed += 1
        else:
            skipped += 1
        checked += 1
        init_count += 1
        #load docs in batches of 10
        print(f"init_percent: {percent}")
    return tasks

def prepare(vectordb, start, amt):
    results = split_texts(vectordb, start, amt)
    vectordb.persist()
#param start - the percentage to start at
#param amt - the percentage to process
#def __init__(self, start, amt):
model_name = "hkunlp/instructor-xl"
model_kwargs = {'device': 'cuda:0'}
encode_kwargs = {'normalize_embeddings': True}
hf = HuggingFaceInstructEmbeddings(
    model_name=model_name,
    model_kwargs=model_kwargs
)
vectordb = Chroma(embedding_function=hf, collection_name="eric_gov_english__peerreviewed", persist_directory="../eric_gov_english_db")
vectordb.persist()


if __name__ == '__main__':
    # Check if the correct number of arguments is provided
    #if len(sys.argv) != 3:
    #    sys.exit(1)

    # Retrieve the command-line arguments
    start = 0 #float(sys.argv[1])
    amt = 10000# float(sys.argv[2])
    print(f"start: {start}")
    print(f"end: {amt}")
    prepare(vectordb, start/100, amt/100)