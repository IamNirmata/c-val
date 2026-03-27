#!/usr/bin/env bash
set -euo pipefail

kubectl delete vcjob -n gcr-admin "$1"
