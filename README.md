# LLM-Based Kubernetes Scheduler Configurator

This project implements an intelligent "Control Loop" for Kubernetes. It uses a local LLM (TinyLlama) to analyze raw cluster metrics and automatically update the scheduling policy via a ConfigMap.

## 1. Prerequisites

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

---

## 2. Step-by-Step Testing Guide

### Step 1: Create the Cluster
Open a terminal and create a multi-node cluster using Kind.
```powershell
kind create cluster --name llm-test
```
*Verification:* Run `kubectl get nodes`. You should see one control-plane node.

### Step 2: Prepare the ConfigMap
The script manages a ConfigMap named `llm-scheduler-config`. Create it manually first to watch it change:
```powershell
kubectl create configmap llm-scheduler-config --from-literal=active_policy=FIFO
```

### Step 3: Run the Configurator Script
Launch your Python script. The LLM will begin "sensing" the cluster and making decisions every 10 seconds(default). 
Provide `--sleep` value for custom time intervals.
```powershell
python test_script.py --sleep 5
```
### Step 4: Simulate Cluster Load
In a **new terminal window**, observe how the LLM reacts to different cluster states.

**Scenario A: High Load (Batch Processing) PLACEHOLDER**
```powershell
kubectl create deployment test-load --image=nginx --replicas=15
```

**Scenario B: Low Load (Latency Sensitive) PLACEHOLDER**
```powershell
kubectl delete deployment test-load
```

## 5. Cleaning Up
When you are finished testing, delete the cluster.
```powershell
kind delete cluster --name llm-test