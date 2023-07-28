import json
import os, sys
from langchain.document_loaders import PyPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter

def split_text(id):
    text_splitter_large = RecursiveCharacterTextSplitter(chunk_size=600, chunk_overlap=0)
    text_splitter_medium = RecursiveCharacterTextSplitter(chunk_size=400, chunk_overlap=0)
    text_splitter_small = RecursiveCharacterTextSplitter(chunk_size=250, chunk_overlap=0)
    #loader = PyPDFLoader(f"{pdf_directory}/{elem['id']}.pdf")
    loaddir = f"{fromdir}/{id}.pdf"
    print(f"loading: {loaddir}")
    loader = PyPDFLoader(loaddir)
    pages = loader.load()
    print(f"done loading: {loaddir}")
    splitdoc_large = text_splitter_large.split_documents(pages)
    splitdoc_medum = text_splitter_medium.split_documents(pages)
    splitdoc_small = text_splitter_small.split_documents(pages)
    parsedoc = {'large_chunks': [], 'medium_chunks': [], 'small_chunks': []}
    for chunk in splitdoc_large:
        parsedoc['large_chunks'].append({"page_content": chunk.page_content, "metadata": chunk.metadata})
    for chunk in splitdoc_medum:
        parsedoc['medium_chunks'].append({"page_content": chunk.page_content, "metadata": chunk.metadata})
    for chunk in splitdoc_small:
        parsedoc['small_chunks'].append({"page_content": chunk.page_content, "metadata": chunk.metadata})
    
    json.dump(parsedoc, open(f"{todir}/{id}.json", "w"))
    return splitdoc

fromdir = "../eric_gov"
todir = "../eric_gov_textified"

if __name__ == '__main__':
    # Check if the correct number of arguments is provided
    if len(sys.argv) != 2:
        print("Usage: python textify.py <id>")
        sys.exit(1)

    # Retrieve the command-line arguments
    id = sys.argv[1]
    print(f"id: {id}")

    # Call the function with the parsed arguments
    split_text(id)