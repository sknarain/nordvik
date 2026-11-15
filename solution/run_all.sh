#!/bin/bash
echo "Building currency exchange data"
python3 currency_scd.py

echo "Running data cleanup"
python3 data_cleansing.py

echo "Building aggregated data"
python3 build_aggregates.py
python3 service_contract_analysis.py

echo "Creating doc embeddings"
#python3 load_docs.py

echo "Run AI chat agent"
python3 nordvik_agent.py
