# LLM-Based Kubernetes Scheduler Advisor

This project implements a dynamic, LLM-driven Kubernetes scheduling policy engine. It uses a local Large Language Model (TinyLlama) to continuously analyze cluster state and queue depth, recommending and dynamically applying scheduling policies (FIFO, PRIORITY, BIN_PACKING, SPREAD) in real-time.

## Architecture

The system is split into two main components:
1. **The Control Plane (`test_script.py` & `llm_scheduler_local.py`)**: Runs locally, queries the Kubernetes API for a cluster snapshot, uses `llama.cpp` to determine the best scheduling policy based on current load, and writes the decision to a Kubernetes ConfigMap.
2. **The Enforcement Plane (`webhook.py`)**: A Mutating Admission Webhook running inside the cluster. It intercepts newly created Pods, reads the active policy from the ConfigMap, and dynamically injects the required scheduling rules (e.g., `priorityClassName`, `affinity`, `topologySpreadConstraints`) before the Pod is saved to the cluster.

## Prerequisites

### Windows Hardware & Software
* **Docker Desktop:** [Download and Install](https://www.docker.com/products/docker-desktop/). Ensure it is running and set to use **WSL 2**.
* **Python 3.10+:** Ensure `python` is in your PATH.
* **Kind (Kubernetes in Docker):**
  ```powershell
  winget install -e --id Kubernetes.kind
  ```

### Python Dependencies
Install the required libraries for Kubernetes communication and local LLM inference:
```powershell
pip install kubernetes llama-cpp-python
```

### The Model
1. Create a folder named `models` in your project directory.
2. Download `tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf` from HuggingFace and place it in that folder.
3. Additional models for testing need to be added to `models` and edit `llm_scheduler_local.py` variables.

## Quick Start Guide

Open your terminal in the root directory of this project and follow these steps to spin up the entire environment.

### 1. Start the Kind Cluster
Create the custom `kind` cluster using your configuration file:
```bash
kind create cluster --name llm-test --config cluster-config.yaml
```

### 2. Build and Load the Webhook
Build the Mutating Admission Webhook Docker image locally and sideload it directly into your `kind` cluster:
```bash
docker build -t llm-scheduler-webhook:latest .
kind load docker-image llm-scheduler-webhook:latest --name llm-test
```

### 3. Generate TLS Certificates & Deploy
Kubernetes requires webhooks to run over HTTPS. Run the included bash script to generate a local Certificate Authority, sign the webhook's certificates, and generate the final deployment manifest:
```bash
chmod +x generate-certs.sh
./generate-certs.sh

# Apply the generated manifest to the cluster
kubectl apply -f webhook-manifest.yaml
```

Wait for the webhook to become fully ready:
```bash
kubectl wait --namespace default --for=condition=ready pod --selector=app=llm-webhook --timeout=90s
```

### 4. Install Python Dependencies
Install the required packages for the local control loop:
```bash
pip install -r requirements.txt
pip install kubernetes llama-cpp-python
```

### 5. Start the LLM Control Loop
Run the main control script. This will start an infinite loop that checks the cluster state every 15 seconds (configurable) and updates the active scheduling policy.
```bash
python test_script.py --sleep 15
```

### Cleaning up
When you are finished testing, delete the cluster.
```powershell
kind delete cluster --name llm-test