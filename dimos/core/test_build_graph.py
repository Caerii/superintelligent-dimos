# Copyright 2025-2026 Dimensional Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest

from dimos.core.blueprints import BlueprintGraph, GraphEdge, GraphNode, autoconnect
from dimos.core.module import Module
from dimos.core.stream import In, Out


class MsgA:
    pass


class MsgB:
    pass


class MsgC:
    pass


class Producer(Module):
    data_a: Out[MsgA]
    data_b: Out[MsgB]


class Consumer(Module):
    data_a: In[MsgA]
    data_b: In[MsgB]
    result: Out[MsgC]


class Sink(Module):
    result: In[MsgC]


class Orphan(Module):
    lonely_out: Out[MsgA]
    lonely_in: In[MsgC]


# --- Tests ---


def test_build_graph_basic_connection() -> None:
    """Connected streams produce edges between modules."""
    bp = autoconnect(Producer.blueprint(), Consumer.blueprint())
    graph = bp.build_graph()

    assert isinstance(graph, BlueprintGraph)

    # Two module nodes
    module_nodes = [n for n in graph.nodes if n.kind == "module"]
    assert len(module_nodes) == 2
    module_ids = {n.id for n in module_nodes}
    assert module_ids == {"Producer", "Consumer"}

    # Two connected edges (data_a and data_b)
    connected_edges = [e for e in graph.edges if e.connected]
    assert len(connected_edges) == 2
    stream_names = {e.stream_name for e in connected_edges}
    assert stream_names == {"data_a", "data_b"}

    for edge in connected_edges:
        assert edge.source == "Producer"
        assert edge.target == "Consumer"
        assert edge.original_name is None  # no remapping
        assert edge.remapped_name is None


def test_build_graph_chain() -> None:
    """Three modules in a chain: Producer -> Consumer -> Sink."""
    bp = autoconnect(Producer.blueprint(), Consumer.blueprint(), Sink.blueprint())
    graph = bp.build_graph()

    module_nodes = [n for n in graph.nodes if n.kind == "module"]
    assert len(module_nodes) == 3

    connected_edges = [e for e in graph.edges if e.connected]
    assert len(connected_edges) == 3  # data_a, data_b, result

    # Check result edge goes Consumer -> Sink
    result_edges = [e for e in connected_edges if e.stream_name == "result"]
    assert len(result_edges) == 1
    assert result_edges[0].source == "Consumer"
    assert result_edges[0].target == "Sink"


def test_build_graph_detached_streams() -> None:
    """Streams with no counterpart appear as detached."""
    bp = autoconnect(Orphan.blueprint())
    graph = bp.build_graph()

    module_nodes = [n for n in graph.nodes if n.kind == "module"]
    assert len(module_nodes) == 1
    assert module_nodes[0].id == "Orphan"

    detached_nodes = [n for n in graph.nodes if n.kind == "detached"]
    assert len(detached_nodes) == 2  # lonely_out has no consumer, lonely_in has no producer

    detached_edges = [e for e in graph.edges if not e.connected]
    assert len(detached_edges) == 2

    # One edge goes out from Orphan (lonely_out), one comes in to Orphan (lonely_in)
    out_edges = [e for e in detached_edges if e.source == "Orphan"]
    in_edges = [e for e in detached_edges if e.target == "Orphan"]
    assert len(out_edges) == 1
    assert len(in_edges) == 1
    assert out_edges[0].stream_name == "lonely_out"
    assert in_edges[0].stream_name == "lonely_in"


def test_build_graph_with_remapping() -> None:
    """Remapped streams show both original and remapped names."""

    class Src(Module):
        color_image: Out[MsgA]

    class Dst(Module):
        remapped_data: In[MsgA]

    bp = autoconnect(Src.blueprint(), Dst.blueprint()).remappings(
        [(Src, "color_image", "remapped_data")]
    )
    graph = bp.build_graph()

    connected_edges = [e for e in graph.edges if e.connected]
    assert len(connected_edges) == 1

    edge = connected_edges[0]
    assert edge.source == "Src"
    assert edge.target == "Dst"
    assert edge.stream_name == "remapped_data"
    assert edge.original_name == "color_image"
    assert edge.remapped_name == "remapped_data"
    assert "color_image" in edge.label  # label mentions the original
    assert "MsgA" in edge.label  # label mentions the type


def test_build_graph_partial_detached() -> None:
    """When one module connects and another doesn't, detached still shows."""
    bp = autoconnect(
        Producer.blueprint(),
        Consumer.blueprint(),
        Orphan.blueprint(),
    )
    graph = bp.build_graph()

    # Producer.data_a connects to both Consumer.data_a and Orphan has lonely_out (also MsgA)
    # But lonely_out name differs from data_a, so it's detached
    module_nodes = [n for n in graph.nodes if n.kind == "module"]
    assert len(module_nodes) == 3

    connected_edges = [e for e in graph.edges if e.connected]
    detached_edges = [e for e in graph.edges if not e.connected]

    # data_a connects Producer->Consumer, data_b connects Producer->Consumer,
    # result connects Consumer->Orphan (Orphan.lonely_in is MsgC, Consumer.result is MsgC => connected!)
    # lonely_out (MsgA out) has no consumer with name lonely_out => detached
    connected_names = {e.stream_name for e in connected_edges}
    assert "data_a" in connected_names
    assert "data_b" in connected_names

    # lonely_out should be detached (no consumer named lonely_out)
    detached_out = [e for e in detached_edges if e.stream_name == "lonely_out"]
    assert len(detached_out) == 1


def test_build_graph_mermaid_output() -> None:
    """Mermaid output is valid-looking."""
    bp = autoconnect(Producer.blueprint(), Consumer.blueprint())
    graph = bp.build_graph()
    mermaid = graph.to_mermaid()

    assert mermaid.startswith("graph LR")
    assert "Producer" in mermaid
    assert "Consumer" in mermaid
    assert "-->|" in mermaid  # connected edge
    assert "classDef module" in mermaid
    assert "classDef detached" in mermaid


def test_build_graph_mermaid_detached_style() -> None:
    """Detached edges use dotted style in mermaid."""
    bp = autoconnect(Orphan.blueprint())
    graph = bp.build_graph()
    mermaid = graph.to_mermaid()

    assert "-.->|" in mermaid  # dotted edge for detached


def test_build_graph_fan_out() -> None:
    """One output connecting to multiple consumers."""

    class Source(Module):
        data_a: Out[MsgA]

    class Sink1(Module):
        data_a: In[MsgA]

    class Sink2(Module):
        data_a: In[MsgA]

    bp = autoconnect(Source.blueprint(), Sink1.blueprint(), Sink2.blueprint())
    graph = bp.build_graph()

    connected_edges = [e for e in graph.edges if e.connected]
    assert len(connected_edges) == 2
    targets = {e.target for e in connected_edges}
    assert targets == {"Sink1", "Sink2"}
    for edge in connected_edges:
        assert edge.source == "Source"


def test_build_graph_no_modules() -> None:
    """Empty blueprint produces empty graph."""
    from dimos.core.blueprints import Blueprint

    bp = Blueprint(blueprints=())
    graph = bp.build_graph()

    assert len(graph.nodes) == 0
    assert len(graph.edges) == 0


def test_build_graph_remapping_both_sides() -> None:
    """Both producer and consumer are remapped to the same name."""

    class A(Module):
        original_out: Out[MsgA]

    class B(Module):
        original_in: In[MsgA]

    bp = autoconnect(A.blueprint(), B.blueprint()).remappings([
        (A, "original_out", "shared"),
        (B, "original_in", "shared"),
    ])
    graph = bp.build_graph()

    connected_edges = [e for e in graph.edges if e.connected]
    assert len(connected_edges) == 1
    edge = connected_edges[0]
    assert edge.stream_name == "shared"
    # Both sides were remapped, so label should mention both originals
    assert "original_out" in edge.label
    assert "original_in" in edge.label
