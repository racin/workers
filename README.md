# Workers

## How to install

```
helm install -f helm/values.yaml --namespace demo --create-namespace notifi helm/
```

## Setup port forward
```
kubectl port-forward pod/notifi-master-0 -n demo 8080:8080
```
## Scale workers
If the new **replicaCount** is lower than the existing value, the residue jobs will be distributed amongst the remaining workers.

If the **replicaCount** is higher than the existing value, the jobs will not be automatically distributed to the new workers. See [this command](#distribute-all-work) to distribute the jobs.
```
helm upgrade -f helm/values.yaml --namespace demo notifi helm/ --set replicaCount=50
```

## Add random jobs

```
curl -X GET http://localhost:8080/addrandomjobs?count=50
```

## Distribute all work
```
curl -X GET http://localhost:8080/distributeallwork
```

## Remove jobs

```
curl -X POST http://localhost:8080/removejobs --data '[{"ID":6874967465064488309},{"ID":6725505124774569258}]'
```
