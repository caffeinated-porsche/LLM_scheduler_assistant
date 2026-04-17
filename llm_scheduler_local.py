"""
llm_scheduler_local.py

Self-contained LLM-based Kubernetes scheduling policy selector.
"""

import json
import os
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional

os.environ.setdefault("LLAMA_N_THREADS_BATCH", "12")


class WorkloadType(str, Enum):
    COMPUTE_HEAVY     = "compute_heavy"
    LATENCY_SENSITIVE = "latency_sensitive"

class Policy(str, Enum):
    FIFO         = "FIFO"
    PRIORITY     = "PRIORITY"
    BIN_PACKING  = "BIN_PACKING"
    SPREAD       = "SPREAD"
    GANG         = "GANG"

@dataclass
class NodeState:
    node_id:                str
    cpu_utilization:        float
    memory_utilization:     float
    pod_count:              int
    available_cpu_cores:    float
    available_memory_gb:    float

@dataclass
class WorkloadDescriptor:
    workload_type:          WorkloadType
    queue_depth:            int
    avg_cpu_request:        float
    avg_memory_request_gb:  float
    has_deadline:           bool
    deadline_seconds:       Optional[float] = None
    pod_group_size:         Optional[int]   = None

@dataclass
class ClusterSnapshot:
    workload:                       WorkloadDescriptor
    nodes:                          list[NodeState]
    recent_p99_latency_ms:          Optional[float] = None
    recent_throughput_pods_per_min: Optional[float] = None

    def summarize(self) -> str:
        avg_cpu = sum(n.cpu_utilization for n in self.nodes) / len(self.nodes)
        avg_mem = sum(n.memory_utilization for n in self.nodes) / len(self.nodes)
        node_summary = f"{len(self.nodes)} nodes (avg CPU {avg_cpu:.0%}, avg mem {avg_mem:.0%})"

        lines = [
            f"Workload type    : {self.workload.workload_type.value}",
            f"Queue depth      : {self.workload.queue_depth} pods",
            f"Avg CPU request  : {self.workload.avg_cpu_request} cores",
            f"Avg mem request  : {self.workload.avg_memory_request_gb} GB",
            f"Deadline         : {'yes' if self.workload.has_deadline else 'none'}",
            f"Pod group size   : {self.workload.pod_group_size or 'N/A'}",
            f"Cluster          : {node_summary}",
        ]
        return "\n".join(lines)

@dataclass
class SchedulingDecision:
    recommended_policy: Policy
    raw_response:       str = field(default="", repr=False)

@dataclass
class TimedDecision:
    decision:            SchedulingDecision
    inference_ms:        float
    tokens_generated:    int
    tokens_per_second:   float

    def __str__(self) -> str:
        return (
            f"Decision   : {self.decision.recommended_policy.value}\n"
            f"Latency    : {self.inference_ms:.1f} ms "
            f"({self.tokens_per_second:.1f} tok/s, {self.tokens_generated} tokens)"
        )

SYSTEM_PROMPT = """\
You are a Kubernetes scheduling policy advisor.
Given a cluster snapshot, pick the best policy: FIFO, PRIORITY, BIN_PACKING, SPREAD, or GANG.

Reply with ONLY the policy name. No prose, no JSON, no markdown.
"""

FEW_SHOT_EXAMPLES: list[dict] = [
    {
        "snapshot": "Workload type: compute_heavy\nQueue depth: 120 pods\nAvg CPU: 4.0\nCluster: 8 nodes (avg CPU 35%)",
        "decision": "BIN_PACKING",
    },
    {
        "snapshot": "Workload type: latency_sensitive\nQueue depth: 18 pods\nAvg CPU: 0.5\nRecent p99 lat: 180.0 ms",
        "decision": "SPREAD",
    },
    {
        "snapshot": "Workload type: compute_heavy\nPod group size: 8\nCluster: 8 nodes",
        "decision": "GANG",
    },
    {
        "snapshot": "Workload type: latency_sensitive\nQueue depth: 50 pods\nDeadline: yes\nCluster: 4 nodes (avg CPU 80%)",
        "decision": "PRIORITY",
    },
    {
        "snapshot": "Workload type: compute_heavy\nQueue depth: 5 pods\nCluster: 4 nodes (avg CPU 92%)",
        "decision": "SPREAD",
    },
    {
        "snapshot": "Workload type: latency_sensitive\nQueue depth: 2 pods\nCluster: 10 nodes (avg CPU 5%)",
        "decision": "FIFO",
    }
]

def _build_messages(snapshot: ClusterSnapshot, n_examples: int = 2) -> list[dict]:
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for ex in FEW_SHOT_EXAMPLES[:n_examples]:
        messages.append({"role": "user", "content": f"Cluster snapshot:\n{ex['snapshot']}"})
        messages.append({"role": "assistant", "content": ex["decision"]})
    messages.append({"role": "user", "content": f"Cluster snapshot:\n{snapshot.summarize()}"})
    return messages


def _parse_response(raw: str) -> SchedulingDecision:
    cleaned = raw.strip().upper()

    # Auto-correct common TinyLlama typos
    if "BIZ" in cleaned:
        cleaned = cleaned.replace("BIZ", "BIN")

    pattern = r"(FIFO|PRIORITY|BIN_PACKING|SPREAD|GANG)"
    match = re.search(pattern, cleaned)

    if not match:
        raise RuntimeError(f"LLM failed to return a valid policy. Raw: '{raw}'")

    return SchedulingDecision(
        recommended_policy=Policy(match.group(1)),
        raw_response=raw
    )

def load_model(model_path: str | Path, **kwargs):
    try:
        from llama_cpp import Llama
    except ImportError:
        raise ImportError("Run: pip install llama-cpp-python")

    return Llama(
        model_path=str(model_path),
        n_ctx=kwargs.get("n_ctx", 1024),
        n_threads=kwargs.get("n_threads", 12),
        n_threads_batch=kwargs.get("n_threads_batch", 12),
        verbose=kwargs.get("verbose", False)
    )

def query_llm(snapshot: ClusterSnapshot, llm, n_examples: int = 2) -> TimedDecision:
    messages = _build_messages(snapshot, n_examples=n_examples)

    t0 = time.perf_counter()
    response = llm.create_chat_completion(
        messages=messages,
        max_tokens=10,
        temperature=0.0,
        stop=["\n", "<|user|>"]
    )
    elapsed_ms = (time.perf_counter() - t0) * 1000

    raw = response["choices"][0]["message"]["content"]
    usage = response.get("usage", {})
    tokens_out = usage.get("completion_tokens", 0)
    tok_per_sec = (tokens_out / elapsed_ms * 1000) if elapsed_ms > 0 else 0.0

    return TimedDecision(
        decision=_parse_response(raw),
        inference_ms=elapsed_ms,
        tokens_generated=tokens_out,
        tokens_per_second=tok_per_sec
    )

if __name__ == "__main__":
    test_snapshot = ClusterSnapshot(
        workload=WorkloadDescriptor(
            workload_type=WorkloadType.LATENCY_SENSITIVE,
            queue_depth=25,
            avg_cpu_request=0.5,
            avg_memory_request_gb=1.0,
            has_deadline=True
        ),
        nodes=[
            NodeState("n1", 0.85, 0.70, 15, 1.0, 2.0),
            NodeState("n2", 0.80, 0.65, 12, 1.2, 2.5),
        ],
        recent_p99_latency_ms=250.0
    )

    MODEL_PATH = "./models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf"

    if not Path(MODEL_PATH).exists():
        print(f"Error: Model not found at {MODEL_PATH}")
    else:
        llm = load_model(MODEL_PATH)
        print("\n=== Running Inference ===")
        result = query_llm(test_snapshot, llm)
        print(result)