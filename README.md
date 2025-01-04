# Collector
Collector component to gather ECJ judgments, summarize them, and upsert them to a Qdrant vector DB.

# Setup
## Prerequisites (besides necessary Python modules)
* MySQL Server
* Qdrant Server
* OpenAI API Key
* Google Gemini API Key (only Gemini was able to summarize even the longest judgments).

## Setup of Chrome Headless
To gather judgments from the ECJ, a Chrome Headless Driver is needed. This is because the relevant content is loaded via JavaScript.

## Configuration
1. Copy _example.settings.py_ to _settings.py_ and enter the relevant settings.

## Usage
Currently, there is no automated pipeline to perform all the necessary steps to upload the judgments to the vector DB.  
Basically, there are several scripts like _1_curia_parser_from_url.py_, where the name explains what the script does. To upload a judgment to the vector DB, you run the scripts from 1-4 sequentially.  
Scripts with two numbers like _3_1_get_judgment.py_ are scripts that are called by other scripts, so you do not need to run them manually.

-----
# Current limitations
* Summarization is not yet implemented. Therefore, no judgments are upserted unless you implement a summarization yourself before trying to upsert the judgments.
