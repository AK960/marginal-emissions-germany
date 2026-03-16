#!/bin/bash

TARGET_DIR="results/msar/"
echo "Create a compressed duplicate of coefficients.csv in $TARGET_DIR..."

find "$TARGET_DIR" -name "coefficients.csv" -exec sh -c 'gzip -c "$1" > "$1.gz"' _ {} \;

echo "Done!"