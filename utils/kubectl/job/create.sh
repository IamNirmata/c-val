#!/bin/bash

# Check if a file argument is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <yaml-file>"
    echo "Example: $0 deployment.yaml"
    exit 1
fi

# Check if the file exists
if [ ! -f "$1" ]; then
    echo "Error: File '$1' does not exist"
    exit 1
fi

# Create Kubernetes resources from the specified file
kubectl create -f "$1"