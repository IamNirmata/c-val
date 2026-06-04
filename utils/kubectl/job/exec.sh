podname=$1
kubectl exec -it "$podname" -n gcr-admin -- /bin/bash