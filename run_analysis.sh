#!/bin/bash

# check if config.env exists
if [ ! -f "config.env" ]; then
    echo "❌ config.env not found"
    echo "please copy config.env.template to config.env and fill in the database connection information"
    echo ""
    echo "execute the following commands:"
    echo "  cp config.env.template config.env"
    echo "  vim config.env"
    exit 1
fi

# load config.env
export $(cat config.env | grep -v '^#' | xargs)

echo "✓ config.env loaded"
echo "  database: $DB_NAME"
echo "  host: $DB_HOST:$DB_PORT"
echo ""

# check Python dependencies
echo "check Python dependencies..."
python3 -c "import pymysql" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "❌ missing necessary Python packages"
    echo "execute: pip install -r requirements_profiler.txt"
    exit 1
fi
echo "✓ Python dependencies checked"
echo ""

# create output directory
OUTPUT_DIR=${OUTPUT_DIR:-"./output"}
mkdir -p "$OUTPUT_DIR"
echo "✓ output directory: $OUTPUT_DIR"
echo ""

# run data feature analysis
echo "========================================== step 1/2: run data feature analysis ==========================================\n"
python3 production_data_profiler.py

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ data feature analysis completed"
    # move output file to output directory
    if [ -f "production_data_profile.json" ]; then
        mv production_data_profile.json "$OUTPUT_DIR/"
        echo "  result file: $OUTPUT_DIR/production_data_profile.json"
    fi
else
    echo ""
    echo "❌ data feature analysis failed"
    exit 1
fi

echo ""
echo "========================================== step 2/2: run data relationship analysis ==========================================\n"
python3 data_relationship_analyzer.py

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ data relationship analysis completed"
    # move output file to output directory
    if [ -f "data_relationship_analysis.json" ]; then
        mv data_relationship_analysis.json "$OUTPUT_DIR/"
        echo "  result file: $OUTPUT_DIR/data_relationship_analysis.json"
    fi
else
    echo ""
    echo "❌ data relationship analysis failed"
    exit 1
fi

echo ""
echo "=========================================="
echo "analysis completed"
echo "=========================================="
echo ""
echo "output files located at: $OUTPUT_DIR/"
echo "  - production_data_profile.json (data feature analysis)"
echo "  - data_relationship_analysis.json (data relationship analysis)"
echo "" 
echo "next step:"
echo "  1. check the output JSON files"
echo "  2. write data generation script based on the analysis results"
echo "  3. generate simulated data for testing"
echo ""