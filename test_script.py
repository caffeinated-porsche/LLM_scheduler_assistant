import time
import os
import re
import argparse
import csv
import json
from dataclasses import asdict
from kubernetes import client, config
from llm_scheduler_local import ClusterSnapshot, WorkloadDescriptor, NodeState, WorkloadType, load_model, query_llm

def parse_cpu(cpu_str: str) -> float:
    if cpu_str.endswith('m'):
        return float(cpu_str[:-1]) / 1000.0
    return float(cpu_str)


def parse_mem(mem_str: str) -> float:
    factors = {'Ki': 1024, 'Mi': 1024 ** 2, 'Gi': 1024 ** 3, 'Ti': 1024 ** 4}
    suffix = mem_str[-2:]
    if suffix in factors:
        bytes_val = float(mem_str[:-2]) * factors[suffix]
    else:
        bytes_val = float(mem_str)
    return bytes_val / (1024 ** 3)


def log_decision(policy, latency, tokens, snapshot: ClusterSnapshot):
    log_file = "scheduler_decisions.log"
    file_exists = os.path.isfile(log_file)

    full_snapshot_json = json.dumps(asdict(snapshot))

    with open(log_file, mode='a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["Timestamp", "Policy", "Inference_ms", "Tokens", "Full_Snapshot_JSON"])

        writer.writerow([
            time.strftime("%Y-%m-%d %H:%M:%S"),
            policy,
            f"{latency:.2f}",
            tokens,
            full_snapshot_json
        ])

class RawK8sConfigurator:
    def __init__(self):
        try:
            config.load_kube_config()
        except:
            config.load_incluster_config()
        self.v1 = client.CoreV1Api()

    def get_cluster_snapshot(self) -> ClusterSnapshot:
        nodes = self.v1.list_node().items
        node_states = []

        for n in nodes:
            cpu_cap = parse_cpu(n.status.capacity['cpu'])
            mem_cap = parse_mem(n.status.capacity['memory'])

            field_selector = f"spec.nodeName={n.metadata.name}"
            pods_on_node = self.v1.list_pod_for_all_namespaces(field_selector=field_selector).items

            req_cpu = 0.0
            req_mem = 0.0
            for p in pods_on_node:
                if p.status.phase == "Running":
                    for container in p.spec.containers:
                        res = container.resources.requests or {}
                        req_cpu += parse_cpu(res.get('cpu', '0m'))
                        req_mem += parse_mem(res.get('memory', '0Mi'))

            node_states.append(NodeState(
                node_id=n.metadata.name,
                cpu_utilization=min(req_cpu / cpu_cap, 1.0) if cpu_cap > 0 else 0,
                memory_utilization=min(req_mem / mem_cap, 1.0) if mem_cap > 0 else 0,
                pod_count=len(pods_on_node),
                available_cpu_cores=max(cpu_cap - req_cpu, 0),
                available_memory_gb=max(mem_cap - req_mem, 0)
            ))

        pending_pods = self.v1.list_pod_for_all_namespaces(field_selector="status.phase=Pending").items

        queue_depth = len(pending_pods)
        w_type = WorkloadType.LATENCY_SENSITIVE
        detected_group_size = None

        if queue_depth > 0:
            owners = [p.metadata.owner_references[0].uid for p in pending_pods if p.metadata.owner_references]

            if owners:
                from collections import Counter
                most_common_owner, count = Counter(owners).most_common(1)[0]
                if count > 2:
                    detected_group_size = count
                    w_type = WorkloadType.COMPUTE_HEAVY

        return ClusterSnapshot(
            workload=WorkloadDescriptor(
                workload_type=w_type,
                queue_depth=queue_depth,
                avg_cpu_request=1.0,
                avg_memory_request_gb=2.0,
                has_deadline=False,
                pod_group_size=detected_group_size
            ),
            nodes=node_states
        )

    def update_config(self, policy: str):
        cm_name = "llm-scheduler-config"
        namespace = "default"
        meta = client.V1ObjectMeta(name=cm_name)
        body = client.V1ConfigMap(
            api_version="v1",
            kind="ConfigMap",
            metadata=meta,
            data={"active_policy": policy, "last_updated": str(time.ctime())}
        )

        try:
            self.v1.replace_namespaced_config_map(name=cm_name, namespace=namespace, body=body)
            print(f"[*] Cluster Policy Updated to: {policy}")
        except client.exceptions.ApiException as e:
            if e.status == 404:
                self.v1.create_namespaced_config_map(namespace=namespace, body=body)
                print(f"[*] Created new ConfigMap with Policy: {policy}")
            else:
                raise


def run_loop(sleep_interval):
    MODEL_PATH = "./models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf"
    if not os.path.exists(MODEL_PATH):
        print(f"Model missing at {MODEL_PATH}")
        return

    llm = load_model(MODEL_PATH)
    configurator = RawK8sConfigurator()

    print(f"--- LLM K8s Configurator Active (Interval: {sleep_interval}s) ---")
    while True:
        try:
            snapshot = configurator.get_cluster_snapshot()
            result = query_llm(snapshot, llm)

            print(result)

            policy = result.decision.recommended_policy.value

            log_decision(policy, result.inference_ms, result.tokens_generated, snapshot)

            configurator.update_config(policy)

        except Exception as e:
            print(f"Error in control loop: {e}")

        time.sleep(sleep_interval)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LLM-based K8s Scheduler Policy Configurator")
    parser.add_argument("--sleep", type=int, default=10, help="Wait time (seconds)")
    args = parser.parse_args()

    run_loop(args.sleep)