#! /bin/bash
echo "------------------ SAMPLE WITH KEY ------------------" 
cat sample_stream.jsonl | target-mssql --config config.json
echo "------------------ SAMPLE WITHOUT KEY ------------------"
cat unkeyed_stream.jsonl | target-mssql --config config.json