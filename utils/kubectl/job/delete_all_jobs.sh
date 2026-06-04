#!/bin/bash

# Get all vcjobs in the gcr-admin namespace and filter for ones containing "hari-gcr-cluster-validation"
jobs_to_delete=$(kubectl get vcjob -n gcr-admin --no-headers -o custom-columns=NAME:.metadata.name | grep "hari-gcr-cluster-validation")

if [ -z "$jobs_to_delete" ]; then
    echo "No jobs found containing 'hari-gcr-cluster-validation'"
    exit 0
fi

echo "Found jobs to delete:"
echo "$jobs_to_delete"
echo

# Ask for confirmation
read -p "Do you want to delete these jobs? (y/N): " confirm

if [[ $confirm =~ ^[Yy]$ ]]; then
    # Delete each job
    echo "$jobs_to_delete" | while read -r job; do
        if [ -n "$job" ]; then
            echo "Deleting job: $job"
            kubectl delete vcjob -n gcr-admin "$job"
        fi
    done
    echo "Deletion completed."
else
    echo "Operation cancelled."
fi