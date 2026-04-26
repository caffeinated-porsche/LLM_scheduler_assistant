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
from kubernetes import client

os.environ.setdefault("LLAMA_N_THREADS_BATCH", "12")


class WorkloadType(str, Enum):
    COMPUTE_HEAVY = "compute_heavy"
    LATENCY_SENSITIVE = "latency_sensitive"


class Policy(str, Enum):
    FIFO = "FIFO"
    PRIORITY = "PRIORITY"
    BIN_PACKING = "BIN_PACKING"
    SPREAD = "SPREAD"


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


@dataclass
class ClusterSnapshot:
    workload:                       WorkloadDescriptor
    nodes:                          list[NodeState]
    recent_p99_latency_ms:          Optional[float] = None
    recent_throughput_pods_per_min: Optional[float] = None

    def summarize(self) -> str:
        avg_cpu = sum(n.cpu_utilization for n in self.nodes) / len(self.nodes)
        avg_mem = sum(n.memory_utilization for n in self.nodes) / \
            len(self.nodes)
        node_summary = f"{len(self.nodes)} nodes (avg CPU {avg_cpu:.0%}, avg mem {avg_mem:.0%})"

        lines = [
            f"Workload type    : {self.workload.workload_type.value}",
            f"Queue depth      : {self.workload.queue_depth} pods",
            f"Avg CPU request  : {self.workload.avg_cpu_request} cores",
            f"Avg mem request  : {self.workload.avg_memory_request_gb} GB",
            f"Deadline         : {'yes' if self.workload.has_deadline else 'none'}",
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
Given a cluster snapshot, pick the best policy: FIFO, PRIORITY, BIN_PACKING, or SPREAD.

Reply with ONLY the policy name. No prose, no JSON, no markdown.
"""

FEW_SHOT_EXAMPLES = [

    # --- 1. IDLE LATENCY → FIFO ---
    {
        "snapshot": """{"workload": {"workload_type": "latency_sensitive", "avg_cpu_request": 1.0, "avg_memory_request_gb": 2.0, "has_deadline": true}, "nodes": [{"cpu_utilization": 0.01, "memory_utilization": 0.01}, {"cpu_utilization": 0.02, "memory_utilization": 0.02}]}""",
        "decision": "FIFO"
    },

    # --- 2. LATENCY + CONTENTION → SPREAD ---
    {
        "snapshot": """{"workload": {"workload_type": "latency_sensitive", "avg_cpu_request": 0.5, "avg_memory_request_gb": 1.0, "has_deadline": true}, "nodes": [{"cpu_utilization": 0.85, "memory_utilization": 0.80}, {"cpu_utilization": 0.90, "memory_utilization": 0.85}]}""",
        "decision": "SPREAD"
    },

    # --- 3. LATENCY + URGENT DEADLINE → PRIORITY ---
    {
        "snapshot": """{"workload": {"workload_type": "latency_sensitive", "avg_cpu_request": 0.7, "avg_memory_request_gb": 1.5, "has_deadline": true, "deadline_seconds": 60.0}, "nodes": [{"cpu_utilization": 0.60, "memory_utilization": 0.55}, {"cpu_utilization": 0.65, "memory_utilization": 0.60}]}""",
        "decision": "PRIORITY"
    },

    # --- 4. COMPUTE HEAVY + HIGH LOAD → BIN_PACKING ---
    {
        "snapshot": """{"workload": {"workload_type": "compute_heavy", "avg_cpu_request": 2.0, "avg_memory_request_gb": 4.0, "has_deadline": false}, "nodes": [{"cpu_utilization": 0.45, "memory_utilization": 0.50}, {"cpu_utilization": 0.50, "memory_utilization": 0.55}]}""",
        "decision": "BIN_PACKING"
    },

    # --- 5. LOW LOAD COMPUTE → FIFO ---
    {
        "snapshot": """{"workload": {"workload_type": "compute_heavy", "avg_cpu_request": 1.0, "avg_memory_request_gb": 2.0, "has_deadline": false}, "nodes": [{"cpu_utilization": 0.20, "memory_utilization": 0.25}, {"cpu_utilization": 0.25, "memory_utilization": 0.30}]}""",
        "decision": "FIFO"
    },

]


def _build_messages(snapshot: ClusterSnapshot, n_examples: int = 5) -> list[dict]:
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for ex in FEW_SHOT_EXAMPLES[:n_examples]:
        messages.append(
            {"role": "user", "content": f"Cluster snapshot:\n{ex['snapshot']}"})
        messages.append({"role": "assistant", "content": ex["decision"]})
    messages.append(
        {"role": "user", "content": f"Cluster snapshot:\n{snapshot.summarize()}"})
    return messages


def _parse_response(raw: str) -> SchedulingDecision:
    cleaned = raw.strip().upper()

    # Auto-correct common TinyLlama typos
    if "BIZ" in cleaned:
        cleaned = cleaned.replace("BIZ", "BIN")

    pattern = r"(FIFO|PRIORITY|BIN_PACKING|SPREAD)"
    match = re.search(pattern, cleaned)

    if not match:
        raise RuntimeError(
            f"LLM failed to return a valid policy. Raw: '{raw}'")

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


def query_llm(snapshot: ClusterSnapshot, llm, n_examples: int = 5) -> TimedDecision:
    messages = _build_messages(snapshot, n_examples=0)

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
