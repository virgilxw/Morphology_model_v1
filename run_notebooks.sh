#!/bin/bash

# Run 1_tessellation.ipynb and save the output
jupyter nbconvert --to notebook --execute 1_tessellation.ipynb --output 1_tessellation_output.ipynb

# Check if the first notebook ran successfully
if [ $? -ne 0 ]; then
    echo "Failed to execute 1_tessellation.ipynb"
    exit 1
fi

# Run 2-chunking-test.ipynb and save the output
jupyter nbconvert --to notebook --execute 2-chunking-test.ipynb --output 2-chunking-test_output.ipynb

# Check if the second notebook ran successfully
if [ $? -ne 0 ]; then
    echo "Failed to execute 2-chunking-tess.ipynb"
    exit 1
fi

echo "Both notebooks executed successfully."
