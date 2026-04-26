import time
import os
import argparse
import csv
import json
from collections import Counter
from dataclasses import asdict
from typing import Optional

from kubernetes import client, config
from llm_scheduler_local import (
    ClusterSnapshot, WorkloadDescriptor, NodeState,
    WorkloadType, Policy, load_model, query_llm
)

def parse_cpu(cpu_str: str) -> float:
    if cpu_str.endswith('m'):
        return float(cpu_str[:-1]) / 1000.0
    return float(cpu_str)


def parse_mem(mem_str: str) -> float:
    factors = {'Ki': 1024, 'Mi': 1024 ** 2, 'Gi': 1024 ** 3, 'Ti': 1024 ** 4}
    suffix = mem_str[-2:]
    if suffix in factors:
        return float(mem_str[:-2]) * factors[suffix] / (1024 ** 3)
    return float(mem_str) / (1024 ** 3)

def log_decision(policy: str, latency: float, tokens: int,
                 snapshot: ClusterSnapshot, changed: bool):
    log_file = "scheduler_decisions.log"
    file_exists = os.path.isfile(log_file)

    with open(log_file, mode='a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "Timestamp", "Policy", "Changed",
                "Inference_ms", "Tokens", "Full_Snapshot_JSON"
            ])
        writer.writerow([
            time.strftime("%Y-%m-%d %H:%M:%S"),
            policy,
            changed,
            f"{latency:.2f}",
            tokens,
            json.dumps(asdict(snapshot)),
        ])

def apply_policy(policy: Policy, namespace: str = "default") -> dict:
    if policy == Policy.PRIORITY:
        _ensure_priority_class("high-priority", priority_value=1000)
        return {"priorityClassName": "high-priority"}

    elif policy == Policy.FIFO:
        _ensure_priority_class("default-priority", priority_value=0)
        return {"priorityClassName": "default-priority"}

    elif policy == Policy.SPREAD:
        return {
            "topologySpreadConstraints": [{
                "maxSkew": 1,
                "topologyKey": "kubernetes.io/hostname",
                "whenUnsatisfiable": "DoNotSchedule",
            }]
        }

    elif policy == Policy.BIN_PACKING:
        return {
            "affinity": {
                "podAffinity": {
                    "preferredDuringSchedulingIgnoredDuringExecution": [{
                        "weight": 100,
                        "podAffinityTerm": {
                            "topologyKey": "kubernetes.io/hostname"
                        }
                    }]
                }
            }
        }

    return {}


def _ensure_priority_class(name: str, priority_value: int):
    scheduling_v1 = client.SchedulingV1Api()
    try:
        scheduling_v1.read_priority_class(name)
    except client.exceptions.ApiException as e:
        if e.status == 404:
            scheduling_v1.create_priority_class(client.V1PriorityClass(
                metadata=client.V1ObjectMeta(name=name),
                value=priority_value,
                global_default=False,
                description=f"Created by LLM scheduler for {name}",
            ))

class RawK8sConfigurator:
    def __init__(self):
        try:
            config.load_kube_config()
        except Exception:
            config.load_incluster_config()
        self.v1 = client.CoreV1Api()

    def get_cluster_snapshot(self) -> ClusterSnapshot:
        all_nodes = self.v1.list_node().items
        node_states = []

        for n in all_nodes:
            labels = n.metadata.labels or {}
            if ("node-role.kubernetes.io/control-plane" in labels or
                    "node-role.kubernetes.io/master" in labels):
                continue

            cpu_cap = parse_cpu(n.status.capacity['cpu'])
            mem_cap = parse_mem(n.status.capacity['memory'])

            pods_on_node = self.v1.list_pod_for_all_namespaces(
                field_selector=f"spec.nodeName={n.metadata.name}"
            ).items

            req_cpu = req_mem = 0.0
            running_pods = 0
            for p in pods_on_node:
                if p.status.phase == "Running":
                    running_pods += 1
                    for container in p.spec.containers:
                        res = container.resources.requests or {}
                        req_cpu += parse_cpu(res.get('cpu', '0m'))
                        req_mem += parse_mem(res.get('memory', '0Mi'))

            node_states.append(NodeState(
                node_id=n.metadata.name,
                cpu_utilization=min(req_cpu / cpu_cap, 1.0) if cpu_cap > 0 else 0.0,
                memory_utilization=min(req_mem / mem_cap, 1.0) if mem_cap > 0 else 0.0,
                pod_count=running_pods,
                available_cpu_cores=max(cpu_cap - req_cpu, 0.0),
                available_memory_gb=max(mem_cap - req_mem, 0.0),
            ))

        if not node_states:
            raise RuntimeError("No worker nodes found.")

        pending_pods = self.v1.list_pod_for_all_namespaces(
            field_selector="status.phase=Pending"
        ).items
        total_cpu = 0.0
        total_mem = 0.0
        valid_pods = 0

        for p in pending_pods:
            if not p.spec.containers:
                continue

            cpu_sum = 0.0
            mem_sum = 0.0

            for c in p.spec.containers:
                req = c.resources.requests or {}

                cpu_sum += parse_cpu(req.get("cpu", "0m"))
                mem_sum += parse_mem(req.get("memory", "0Mi"))

            total_cpu += cpu_sum
            total_mem += mem_sum
            valid_pods += 1

        if valid_pods > 0:
            avg_cpu = total_cpu / valid_pods
            avg_mem = total_mem / valid_pods
        else:
            avg_cpu = 0.0
            avg_mem = 0.0

        def is_schedulable(pod) -> bool:
            if pod.spec.node_name is not None:
                return False

            conditions = pod.status.conditions or []
            for cond in conditions:
                if cond.type == "PodScheduled" and cond.status == "False":
                    if cond.reason == "Unschedulable":
                        return True

            return False

        relevant_pending = [p for p in pending_pods if is_schedulable(p)]
        queue_depth = len(relevant_pending)

        w_type = WorkloadType.LATENCY_SENSITIVE
        if queue_depth > 0:
            owners = [
                p.metadata.owner_references[0].uid
                for p in relevant_pending if p.metadata.owner_references
            ]
            if owners:
                _, count = Counter(owners).most_common(1)[0]
                if count > 2:
                    w_type = WorkloadType.COMPUTE_HEAVY

        return ClusterSnapshot(
            workload=WorkloadDescriptor(
                workload_type=w_type,
                queue_depth=queue_depth,
                avg_cpu_request=avg_cpu,
                avg_memory_request_gb=avg_mem,
                has_deadline=(w_type == WorkloadType.LATENCY_SENSITIVE),
                deadline_seconds=300.0 if w_type == WorkloadType.LATENCY_SENSITIVE else None,
            ),
            nodes=node_states,
        )

    def update_config(self, policy: str, spec_overrides: dict):
        cm_name   = "llm-scheduler-config"
        namespace = "default"
        body = client.V1ConfigMap(
            api_version="v1",
            kind="ConfigMap",
            metadata=client.V1ObjectMeta(name=cm_name),
            data={
                "active_policy":  policy,
                "spec_overrides": json.dumps(spec_overrides),
                "last_updated":   time.ctime(),
            },
        )
        try:
            self.v1.replace_namespaced_config_map(
                name=cm_name, namespace=namespace, body=body
            )
        except client.exceptions.ApiException as e:
            if e.status == 404:
                self.v1.create_namespaced_config_map(namespace=namespace, body=body)
            else:
                raise
        print(f"[*] ConfigMap updated — active policy: {policy}")
        print(f"[*] Spec overrides  : {list(spec_overrides.keys()) or 'none'}")

class PolicyState:
    """
    Tracks the currently active scheduling policy.
    Only triggers a reconfiguration when the LLM recommends a change.
    """

    def __init__(self, configurator: RawK8sConfigurator):
        self.current_policy: Optional[Policy] = None
        try:
            cm = configurator.v1.read_namespaced_config_map(
                "llm-scheduler-config", "default"
            )
            saved = cm.data.get("active_policy")
            if saved:
                self.current_policy = Policy(saved)
                print(f"[*] Resumed from saved policy: {self.current_policy.value}")
        except Exception:
            print("[*] No existing ConfigMap found, starting fresh")

        self.change_count: int = 0

    def needs_update(self, recommended: Policy) -> bool:
        return recommended != self.current_policy

    def activate(self, recommended: Policy, configurator: RawK8sConfigurator):
        """Apply the new policy — update priority classes and ConfigMap."""
        spec_overrides = apply_policy(recommended)
        configurator.update_config(recommended.value, spec_overrides)
        self.current_policy = recommended
        self.change_count += 1

def run_loop(sleep_interval: int):
    MODEL_PATH = "./models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf"
    if not os.path.exists(MODEL_PATH):
        print(f"[!] Model missing at {MODEL_PATH}")
        return

    llm          = load_model(MODEL_PATH)
    configurator = RawK8sConfigurator()
    state        = PolicyState(configurator)

    print(f"--- LLM K8s Scheduler Active (interval={sleep_interval}s) ---")
    print(f"    Submit jobs manually — the active policy will be applied to pending pods.")
    print()

    while True:
        try:
            snapshot    = configurator.get_cluster_snapshot()
            result      = query_llm(snapshot, llm)
            recommended = result.decision.recommended_policy

            print(result)
            print(f"[*] Current policy : {state.current_policy.value if state.current_policy else 'none'}")
            print(f"[*] Recommended    : {recommended.value}")

            changed = state.needs_update(recommended)

            if changed:
                print(f"[*] Policy change  : "
                      f"{state.current_policy.value if state.current_policy else 'none'} "
                      f"-> {recommended.value}")
                state.activate(recommended, configurator)
            else:
                print(f"[*] No change — policy remains {recommended.value}")

            log_decision(
                recommended.value,
                result.inference_ms,
                result.tokens_generated,
                snapshot,
                changed,
            )
            print()

        except Exception as e:
            print(f"[!] Error in control loop: {e}")

        time.sleep(sleep_interval)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="LLM-based Kubernetes Scheduling Policy Advisor"
    )
    parser.add_argument(
        "--sleep", type=int, default=30,
        help="Seconds between scheduling decisions (default: 30)"
    )
    args = parser.parse_args()
    run_loop(args.sleep)