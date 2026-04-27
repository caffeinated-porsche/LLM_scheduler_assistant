kind create cluster --name llm-test --config cluster-config.yaml
docker build -t llm-scheduler-webhook:latest .
kind load docker-image llm-scheduler-webhook:latest --name llm-test
bash ./generate-certs.sh
kubectl apply -f webhook-manifest.yaml
kubectl wait --namespace default --for=condition=ready pod --selector=app=llm-webhook --timeout=90s
python test_script.py --sleep 15