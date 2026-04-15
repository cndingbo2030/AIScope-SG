"""
Generate a GraphRAG-ready Skill-Occupation-Risk knowledge graph and indices.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import networkx as nx

BASE = Path(__file__).resolve().parent.parent
DATA_JSON = BASE / "web" / "data" / "data.json"
GRAPH_JSON = BASE / "data" / "processed" / "occupation_graph.json"
CORPUS_TXT = BASE / "data" / "processed" / "graph_corpus.txt"
TRIPLES_JSONL = BASE / "data" / "processed" / "triples.jsonl"
KG_INDICES_JSONL = BASE / "data" / "processed" / "kg_indices.jsonl"
WEB_KG_INDICES_JSONL = BASE / "web" / "data" / "kg_indices.jsonl"
WEB_TRIPLES_JSONL = BASE / "web" / "data" / "triples.jsonl"


def normalize_skill_tokens(text: str) -> set[str]:
    parts = [x.strip().lower() for x in text.replace("/", ",").replace(";", ",").split(",")]
    return {p for p in parts if p}


def infer_skill_set(occ: dict[str, Any]) -> set[str]:
    explicit = occ.get("skills", [])
    if isinstance(explicit, list):
        return {str(s).strip().lower() for s in explicit if str(s).strip()}
    if isinstance(explicit, str):
        return normalize_skill_tokens(explicit)

    risk_factor = str(occ.get("risk_factor", ""))
    fallback = normalize_skill_tokens(risk_factor)
    if not fallback:
        fallback = {"customer communication" if not occ.get("wfh", False) else "digital workflows"}
    return fallback


def vulnerability_index(occ: dict[str, Any]) -> float:
    ai_score = float(occ.get("ai_score", 0))
    pwm = bool(occ.get("pwm", False))
    regulated = bool(occ.get("regulated", False))
    wfh = bool(occ.get("wfh", False))
    idx = ai_score / 10.0
    if pwm:
        idx -= 0.2
    if regulated:
        idx -= 0.15
    if wfh:
        idx += 0.08
    return round(max(0.0, min(1.0, idx)), 3)


def build_graph(data: dict[str, Any]) -> nx.MultiDiGraph:
    graph = nx.MultiDiGraph()
    occupations: list[dict[str, Any]] = []

    for category in data.get("children", []):
        sector = category["name"]
        graph.add_node(f"sector::{sector}", node_type="Sector", label=sector)

        for occ in category.get("children", []):
            occ_id = f"occupation::{occ['name']}"
            occupations.append({**occ, "category": sector})
            vuln = vulnerability_index(occ)
            core = int(occ.get("employment", 0)) > 50000
            graph.add_node(
                occ_id,
                node_type="Occupation",
                label=occ["name"],
                ai_score=float(occ.get("ai_score", 0)),
                wage=float(occ.get("gross_wage", 0)),
                vulnerability_index=vuln,
                core_node=core,
                ssoc_code=occ.get("ssoc_code", ""),
                pwm=bool(occ.get("pwm", False)),
            )
            graph.add_edge(occ_id, f"sector::{sector}", rel_type="IN_SECTOR")

            risk_id = f"risk::{occ.get('risk_factor', 'general')}"
            graph.add_node(risk_id, node_type="Risk Factor", label=occ.get("risk_factor", "general"))
            graph.add_edge(occ_id, risk_id, rel_type="HAS_RISK")

            for skill in infer_skill_set(occ):
                skill_id = f"skill::{skill}"
                graph.add_node(skill_id, node_type="Skills", label=skill)
                graph.add_edge(occ_id, skill_id, rel_type="REQUIRES_SKILL")

    # SIMILAR_RISK: share >3 risk/skill tokens
    for i, occ_a in enumerate(occupations):
        a_risk = infer_skill_set(occ_a) | normalize_skill_tokens(str(occ_a.get("risk_factor", "")))
        for occ_b in occupations[i + 1 :]:
            b_risk = infer_skill_set(occ_b) | normalize_skill_tokens(str(occ_b.get("risk_factor", "")))
            overlap = a_risk.intersection(b_risk)
            if len(overlap) > 3:
                graph.add_edge(
                    f"occupation::{occ_a['name']}",
                    f"occupation::{occ_b['name']}",
                    rel_type="SIMILAR_RISK",
                    overlap_count=len(overlap),
                )
                graph.add_edge(
                    f"occupation::{occ_b['name']}",
                    f"occupation::{occ_a['name']}",
                    rel_type="SIMILAR_RISK",
                    overlap_count=len(overlap),
                )

    # TRANSFER_PATH: A high-risk skill maps to B low-risk occupation skill
    for occ_a in occupations:
        if float(occ_a.get("ai_score", 0)) < 7:
            continue
        a_skills = infer_skill_set(occ_a)
        for occ_b in occupations:
            if occ_a["name"] == occ_b["name"]:
                continue
            if float(occ_b.get("ai_score", 0)) > 4.5:
                continue
            b_skills = infer_skill_set(occ_b)
            shared = a_skills.intersection(b_skills)
            if shared:
                graph.add_edge(
                    f"occupation::{occ_a['name']}",
                    f"occupation::{occ_b['name']}",
                    rel_type="TRANSFER_PATH",
                    via=sorted(shared),
                )

    return graph


def graph_to_corpus(graph: nx.MultiDiGraph) -> str:
    lines: list[str] = []
    for node, attrs in graph.nodes(data=True):
        if attrs.get("node_type") != "Occupation":
            continue
        lines.append(f"Occupation: {attrs.get('label')}")
        lines.append(f"  AI score: {attrs.get('ai_score')}")
        lines.append(f"  Median wage: {attrs.get('wage')}")
        lines.append(f"  Vulnerability index: {attrs.get('vulnerability_index')}")
        lines.append(f"  Core node: {attrs.get('core_node')}")

        successors = graph.out_edges(node, keys=True, data=True)
        for _, target, _, edge_attrs in successors:
            rel = edge_attrs.get("rel_type")
            target_label = graph.nodes[target].get("label", target)
            if rel == "IN_SECTOR":
                lines.append(f"  Sector: {target_label}")
            elif rel == "HAS_RISK":
                lines.append(f"  Primary risk: {target_label}")
            elif rel == "REQUIRES_SKILL":
                lines.append(f"  Skill: {target_label}")
            elif rel == "SIMILAR_RISK":
                lines.append(f"  Similar risk with: {target_label} (shared factors: {edge_attrs.get('overlap_count')})")
            elif rel == "TRANSFER_PATH":
                via = ", ".join(edge_attrs.get("via", []))
                lines.append(f"  Transfer path to: {target_label} via skills [{via}]")
        lines.append(
            "  Topology note: This role is positioned in Singapore's labor graph by sector regulation, "
            "automation pressure, and skill transfer adjacency."
        )
        lines.append("")
    return "\n".join(lines)


def emit_triples(graph: nx.MultiDiGraph) -> list[dict[str, Any]]:
    triples: list[dict[str, Any]] = []
    for source, target, _, edge_attrs in graph.edges(keys=True, data=True):
        rel = edge_attrs.get("rel_type")
        if rel not in {"REQUIRES_SKILL", "HAS_RISK", "TRANSFER_PATH", "SIMILAR_RISK", "IN_SECTOR"}:
            continue
        triples.append(
            {
                "head": graph.nodes[source].get("label", source),
                "relation": rel,
                "tail": graph.nodes[target].get("label", target),
                "head_type": graph.nodes[source].get("node_type", "unknown"),
                "tail_type": graph.nodes[target].get("node_type", "unknown"),
            }
        )
    return triples


def best_transfer_targets(graph: nx.MultiDiGraph, node: str, top_n: int = 2) -> list[str]:
    candidates: list[tuple[float, str]] = []
    for _, target, _, attrs in graph.out_edges(node, keys=True, data=True):
        if attrs.get("rel_type") != "TRANSFER_PATH":
            continue
        target_data = graph.nodes[target]
        score = float(target_data.get("ai_score", 0))
        wage = float(target_data.get("wage", 0))
        ranking = wage - (score * 350)
        candidates.append((ranking, target_data.get("label", target)))
    candidates.sort(reverse=True)
    return [label for _, label in candidates[:top_n]]


def emit_kg_indices(graph: nx.MultiDiGraph) -> list[dict[str, Any]]:
    indices: list[dict[str, Any]] = []
    for node, attrs in graph.nodes(data=True):
        if attrs.get("node_type") != "Occupation":
            continue
        transfer = best_transfer_targets(graph, node, top_n=2)
        summary = (
            f"{attrs.get('label')} (SSOC {attrs.get('ssoc_code', 'N/A')}) has AI score {attrs.get('ai_score')}, "
            f"vulnerability index {attrs.get('vulnerability_index')}, PWM={attrs.get('pwm')}, "
            f"core_node={attrs.get('core_node')}. Suggested transition paths: {', '.join(transfer) if transfer else 'none'}."
        )
        indices.append(
            {
                "occupation": attrs.get("label"),
                "ssoc_code": attrs.get("ssoc_code", ""),
                "ai_score": attrs.get("ai_score"),
                "vulnerability_index": attrs.get("vulnerability_index"),
                "pwm": attrs.get("pwm"),
                "core_node": attrs.get("core_node"),
                "enhanced_summary": summary,
                "transition_suggestions": transfer,
            }
        )
    return indices


def main() -> None:
    if not DATA_JSON.exists():
        raise FileNotFoundError(f"Missing data file: {DATA_JSON}")

    data = json.loads(DATA_JSON.read_text(encoding="utf-8"))
    graph = build_graph(data)

    GRAPH_JSON.parent.mkdir(parents=True, exist_ok=True)
    payload = nx.node_link_data(graph)
    GRAPH_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    corpus = graph_to_corpus(graph)
    CORPUS_TXT.write_text(corpus, encoding="utf-8")

    triples = emit_triples(graph)
    with TRIPLES_JSONL.open("w", encoding="utf-8") as f:
        for item in triples:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
    WEB_TRIPLES_JSONL.parent.mkdir(parents=True, exist_ok=True)
    WEB_TRIPLES_JSONL.write_text(TRIPLES_JSONL.read_text(encoding="utf-8"), encoding="utf-8")

    kg_indices = emit_kg_indices(graph)
    with KG_INDICES_JSONL.open("w", encoding="utf-8") as f:
        for item in kg_indices:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
    WEB_KG_INDICES_JSONL.parent.mkdir(parents=True, exist_ok=True)
    WEB_KG_INDICES_JSONL.write_text(KG_INDICES_JSONL.read_text(encoding="utf-8"), encoding="utf-8")

    print(f"Saved graph -> {GRAPH_JSON}")
    print(f"Saved corpus -> {CORPUS_TXT}")
    print(f"Saved triples -> {TRIPLES_JSONL}")
    print(f"Saved web triples -> {WEB_TRIPLES_JSONL}")
    print(f"Saved kg indices -> {KG_INDICES_JSONL}")
    print(f"Saved web kg indices -> {WEB_KG_INDICES_JSONL}")
    print(f"Graph stats: nodes={graph.number_of_nodes()}, edges={graph.number_of_edges()}")


if __name__ == "__main__":
    main()
