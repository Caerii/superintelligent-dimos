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

from abc import ABC
from collections import defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from functools import cached_property, reduce
import operator
import sys
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Literal, get_args, get_origin, get_type_hints

if TYPE_CHECKING:
    from dimos.protocol.service.system_configurator.base import SystemConfigurator

from dimos.core.global_config import GlobalConfig, global_config
from dimos.core.module import Module, ModuleBase, ModuleSpec, is_module_type
from dimos.core.module_coordinator import ModuleCoordinator
from dimos.core.stream import In, Out
from dimos.core.transport import LCMTransport, PubSubTransport, pLCMTransport
from dimos.spec.utils import Spec, is_spec, spec_annotation_compliance, spec_structural_compliance
from dimos.utils.generic import short_id
from dimos.utils.logging_config import setup_logger

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing import Any as Self

logger = setup_logger()


@dataclass(frozen=True)
class StreamRef:
    name: str
    type: type
    direction: Literal["in", "out"]


@dataclass(frozen=True)
class ModuleRef:
    name: str
    spec: type[Spec] | type[ModuleBase]


@dataclass(frozen=True)
class GraphNode:
    id: str
    label: str
    kind: Literal["module", "detached"]


@dataclass(frozen=True)
class GraphEdge:
    source: str
    target: str
    label: str
    stream_name: str
    stream_type: str
    connected: bool
    original_name: str | None = None
    remapped_name: str | None = None


@dataclass(frozen=True)
class BlueprintGraph:
    nodes: tuple[GraphNode, ...]
    edges: tuple[GraphEdge, ...]

    def to_mermaid(self) -> str:
        """Render the graph as a Mermaid diagram string."""
        lines = ["graph LR"]
        for node in self.nodes:
            safe_id = node.id.replace(" ", "_")
            escaped_label = node.label.replace('"', '#quot;')
            if node.kind == "detached":
                lines.append(f'    {safe_id}[/"{escaped_label}"/]:::detached')
            else:
                lines.append(f'    {safe_id}["{escaped_label}"]:::module')
        for edge in self.edges:
            src = edge.source.replace(" ", "_")
            tgt = edge.target.replace(" ", "_")
            escaped_label = edge.label.replace('"', '#quot;')
            if edge.connected:
                lines.append(f'    {src} -->|"{escaped_label}"| {tgt}')
            else:
                lines.append(f'    {src} -.->|"{escaped_label}"| {tgt}')
        lines.append("    classDef module fill:#2d2d2d,stroke:#4a9eff,color:#fff")
        lines.append(
            "    classDef detached fill:#4a2020,stroke:#ff6b6b,color:#ff6b6b,stroke-dasharray: 5 5"
        )
        return "\n".join(lines)

    def open_in_browser(self) -> None:
        """Render to an HTML file with Mermaid and open in the default browser."""
        import os
        import webbrowser

        mermaid_code = self.to_mermaid()
        html = f"""\
<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<title>Blueprint Graph</title>
<style>
html, body {{ margin: 0; padding: 0; height: 100%; overflow: hidden; background: #1e1e1e; }}
#container {{ width: 100%; height: 100%; }}
svg {{ cursor: grab; }}
svg:active {{ cursor: grabbing; }}
</style>
</head><body>
<div id="container">
<pre class="mermaid">
{mermaid_code}
</pre>
</div>
<script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
<script>
mermaid.initialize({{ startOnLoad: true, theme: 'dark' }});

// Pan & zoom after mermaid renders
window.addEventListener('load', () => {{
  setTimeout(() => {{
    const svg = document.querySelector('svg');
    if (!svg) return;
    let scale = 1, panX = 0, panY = 0, dragging = false, startX, startY;

    function applyTransform() {{
      svg.style.transform = `translate(${{panX}}px, ${{panY}}px) scale(${{scale}})`;
      svg.style.transformOrigin = '0 0';
    }}

    svg.addEventListener('wheel', (e) => {{
      e.preventDefault();
      const rect = svg.getBoundingClientRect();
      const mouseX = e.clientX - rect.left;
      const mouseY = e.clientY - rect.top;

      const oldScale = scale;
      const delta = e.deltaY > 0 ? 0.9 : 1.1;
      scale = Math.min(Math.max(0.1, scale * delta), 10);

      panX = e.clientX - (mouseX / oldScale) * scale - (rect.left - panX - e.clientX) * (scale / oldScale - 1);
      panY = e.clientY - (mouseY / oldScale) * scale - (rect.top - panY - e.clientY) * (scale / oldScale - 1);
      panX = e.clientX - ((e.clientX - panX) / oldScale) * scale;
      panY = e.clientY - ((e.clientY - panY) / oldScale) * scale;

      applyTransform();
    }}, {{ passive: false }});

    svg.addEventListener('mousedown', (e) => {{
      dragging = true;
      startX = e.clientX - panX;
      startY = e.clientY - panY;
    }});
    window.addEventListener('mousemove', (e) => {{
      if (!dragging) return;
      panX = e.clientX - startX;
      panY = e.clientY - startY;
      applyTransform();
    }});
    window.addEventListener('mouseup', () => {{ dragging = false; }});
  }}, 500);
}});
</script>
</body></html>"""

        path = os.path.join(os.path.expanduser("~"), "dimos_blueprint_graph.html")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(html)
        print(f"Graph written to {path}")
        webbrowser.open(f"file://{path}")


@dataclass(frozen=True)
class _BlueprintAtom:
    kwargs: dict[str, Any]
    module: type[ModuleBase[Any]]
    streams: tuple[StreamRef, ...]
    module_refs: tuple[ModuleRef, ...]

    @classmethod
    def create(cls, module: type[ModuleBase[Any]], kwargs: dict[str, Any]) -> Self:
        streams: list[StreamRef] = []
        module_refs: list[ModuleRef] = []

        # Resolve annotations using namespaces from the full MRO chain so that
        # In/Out behind TYPE_CHECKING + `from __future__ import annotations` work.
        # Iterate reversed MRO so the most specific class's namespace wins when
        # parent modules shadow names (e.g. spec.perception.Image vs sensor_msgs.Image).
        globalns: dict[str, Any] = {}
        for c in reversed(module.__mro__):
            if c.__module__ in sys.modules:
                globalns.update(sys.modules[c.__module__].__dict__)
        try:
            all_annotations = get_type_hints(module, globalns=globalns)
        except Exception:
            # Fallback to raw annotations if get_type_hints fails.
            all_annotations = {}
            for base_class in reversed(module.__mro__):
                if hasattr(base_class, "__annotations__"):
                    all_annotations.update(base_class.__annotations__)

        for name, annotation in all_annotations.items():
            origin = get_origin(annotation)
            # Streams
            if origin in (In, Out):
                direction = "in" if origin == In else "out"
                type_ = get_args(annotation)[0]
                streams.append(
                    StreamRef(name=name, type=type_, direction=direction)  # type: ignore[arg-type]
                )
            # linking to unknown module via Spec
            elif is_spec(annotation):
                module_refs.append(ModuleRef(name=name, spec=annotation))
            # linking to specific/known module directly
            elif is_module_type(annotation):
                module_refs.append(ModuleRef(name=name, spec=annotation))

        return cls(
            module=module,
            streams=tuple(streams),
            module_refs=tuple(module_refs),
            kwargs=kwargs,
        )


@dataclass(frozen=True)
class Blueprint:
    blueprints: tuple[_BlueprintAtom, ...]
    transport_map: Mapping[tuple[str, type], PubSubTransport[Any]] = field(
        default_factory=lambda: MappingProxyType({})
    )
    global_config_overrides: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))
    remapping_map: Mapping[tuple[type[ModuleBase], str], str | type[ModuleBase] | type[Spec]] = (
        field(default_factory=lambda: MappingProxyType({}))
    )
    requirement_checks: tuple[Callable[[], str | None], ...] = field(default_factory=tuple)
    configurator_checks: "tuple[SystemConfigurator, ...]" = field(default_factory=tuple)

    @classmethod
    def create(cls, module: type[ModuleBase], **kwargs: Any) -> "Blueprint":
        blueprint = _BlueprintAtom.create(module, kwargs)
        return cls(blueprints=(blueprint,))

    def transports(self, transports: dict[tuple[str, type], Any]) -> "Blueprint":
        return Blueprint(
            blueprints=self.blueprints,
            transport_map=MappingProxyType({**self.transport_map, **transports}),
            global_config_overrides=self.global_config_overrides,
            remapping_map=self.remapping_map,
            requirement_checks=self.requirement_checks,
            configurator_checks=self.configurator_checks,
        )

    def global_config(self, **kwargs: Any) -> "Blueprint":
        return Blueprint(
            blueprints=self.blueprints,
            transport_map=self.transport_map,
            global_config_overrides=MappingProxyType({**self.global_config_overrides, **kwargs}),
            remapping_map=self.remapping_map,
            requirement_checks=self.requirement_checks,
            configurator_checks=self.configurator_checks,
        )

    def remappings(
        self,
        remappings: list[
            tuple[type[ModuleBase[Any]], str, str | type[ModuleBase[Any]] | type[Spec]]
        ],
    ) -> "Blueprint":
        remappings_dict = dict(self.remapping_map)
        for module, old, new in remappings:
            remappings_dict[(module, old)] = new

        return Blueprint(
            blueprints=self.blueprints,
            transport_map=self.transport_map,
            global_config_overrides=self.global_config_overrides,
            remapping_map=MappingProxyType(remappings_dict),
            requirement_checks=self.requirement_checks,
            configurator_checks=self.configurator_checks,
        )

    def requirements(self, *checks: Callable[[], str | None]) -> "Blueprint":
        return Blueprint(
            blueprints=self.blueprints,
            transport_map=self.transport_map,
            global_config_overrides=self.global_config_overrides,
            remapping_map=self.remapping_map,
            requirement_checks=self.requirement_checks + tuple(checks),
            configurator_checks=self.configurator_checks,
        )

    def configurators(self, *checks: "SystemConfigurator") -> "Blueprint":
        return Blueprint(
            blueprints=self.blueprints,
            transport_map=self.transport_map,
            global_config_overrides=self.global_config_overrides,
            remapping_map=self.remapping_map,
            requirement_checks=self.requirement_checks,
            configurator_checks=self.configurator_checks + tuple(checks),
        )

    def _check_ambiguity(
        self,
        requested_method_name: str,
        interface_methods: Mapping[str, list[tuple[type[ModuleBase], Callable[..., Any]]]],
        requesting_module: type[ModuleBase],
    ) -> None:
        if (
            requested_method_name in interface_methods
            and len(interface_methods[requested_method_name]) > 1
        ):
            modules_str = ", ".join(
                impl[0].__name__ for impl in interface_methods[requested_method_name]
            )
            raise ValueError(
                f"Ambiguous RPC method '{requested_method_name}' requested by "
                f"{requesting_module.__name__}. Multiple implementations found: "
                f"{modules_str}. Please use a concrete class name instead."
            )

    def _get_transport_for(self, name: str, stream_type: type) -> PubSubTransport[Any]:
        transport = self.transport_map.get((name, stream_type), None)
        if transport:
            return transport

        use_pickled = getattr(stream_type, "lcm_encode", None) is None
        topic = f"/{name}" if self._is_name_unique(name) else f"/{short_id()}"
        transport = pLCMTransport(topic) if use_pickled else LCMTransport(topic, stream_type)

        return transport

    @cached_property
    def _all_name_types(self) -> set[tuple[str, type]]:
        # Apply remappings to get the actual names that will be used
        result = set()
        for blueprint in self.blueprints:
            for conn in blueprint.streams:
                # Check if this stream should be remapped
                remapped_name = self.remapping_map.get((blueprint.module, conn.name), conn.name)
                if isinstance(remapped_name, str):
                    result.add((remapped_name, conn.type))
        return result

    def _is_name_unique(self, name: str) -> bool:
        return sum(1 for n, _ in self._all_name_types if n == name) == 1

    def _run_configurators(self) -> None:
        from dimos.protocol.service.system_configurator import configure_system, lcm_configurators

        configurators = [*lcm_configurators(), *self.configurator_checks]

        try:
            configure_system(configurators)
        except SystemExit:
            labels = [type(c).__name__ for c in configurators]
            print(
                f"Required system configuration was declined: {', '.join(labels)}",
                file=sys.stderr,
            )
            sys.exit(1)

    def _check_requirements(self) -> None:
        errors = []
        red = "\033[31m"
        reset = "\033[0m"

        for check in self.requirement_checks:
            error = check()
            if error:
                errors.append(error)

        if errors:
            for error in errors:
                print(f"{red}Error: {error}{reset}", file=sys.stderr)
            sys.exit(1)

    def _verify_no_name_conflicts(self) -> None:
        name_to_types = defaultdict(set)
        name_to_modules = defaultdict(list)

        for blueprint in self.blueprints:
            for conn in blueprint.streams:
                stream_name = self.remapping_map.get((blueprint.module, conn.name), conn.name)
                name_to_types[stream_name].add(conn.type)
                name_to_modules[stream_name].append((blueprint.module, conn.type))

        conflicts = {}
        for conn_name, types in name_to_types.items():
            if len(types) > 1:
                modules_by_type = defaultdict(list)
                for module, conn_type in name_to_modules[conn_name]:
                    modules_by_type[conn_type].append(module)
                conflicts[conn_name] = modules_by_type

        if not conflicts:
            return

        error_lines = ["Blueprint cannot start because there are conflicting streams."]
        for name, modules_by_type in conflicts.items():
            type_entries = []
            for conn_type, modules in modules_by_type.items():
                for module in modules:
                    type_str = f"{conn_type.__module__}.{conn_type.__name__}"
                    module_str = module.__name__
                    type_entries.append((type_str, module_str))
            if len(type_entries) >= 2:
                locations = ", ".join(f"{type_} in {module}" for type_, module in type_entries)
                error_lines.append(f"    - '{name}' has conflicting types. {locations}")

        raise ValueError("\n".join(error_lines))

    def _deploy_all_modules(
        self, module_coordinator: ModuleCoordinator, global_config: GlobalConfig
    ) -> None:
        module_specs: list[ModuleSpec] = []
        for blueprint in self.blueprints:
            module_specs.append((blueprint.module, global_config, blueprint.kwargs))

        module_coordinator.deploy_parallel(module_specs)

    def _connect_streams(self, module_coordinator: ModuleCoordinator) -> None:
        # dict when given (final/remapped) stream name+type, provides a list of modules + original (non-remapped) stream names
        streams = defaultdict(list)

        for blueprint in self.blueprints:
            for conn in blueprint.streams:
                # Check if this stream should be remapped
                remapped_name = self.remapping_map.get((blueprint.module, conn.name), conn.name)
                if isinstance(remapped_name, str):
                    # Group by remapped name and type
                    streams[remapped_name, conn.type].append((blueprint.module, conn.name))

        # Connect all In/Out streams by remapped name and type.
        for remapped_name, stream_type in streams.keys():
            transport = self._get_transport_for(remapped_name, stream_type)
            for module, original_name in streams[(remapped_name, stream_type)]:
                instance = module_coordinator.get_instance(module)  # type: ignore[assignment]
                instance.set_transport(original_name, transport)  # type: ignore[union-attr]
                logger.info(
                    "Transport",
                    name=remapped_name,
                    original_name=original_name,
                    topic=str(getattr(transport, "topic", None)),
                    type=f"{stream_type.__module__}.{stream_type.__qualname__}",
                    module=module.__name__,
                    transport=transport.__class__.__name__,
                )

    def _connect_module_refs(self, module_coordinator: ModuleCoordinator) -> None:
        # partly fill out the mod_and_mod_ref_to_proxy
        mod_and_mod_ref_to_proxy = {
            (module, name): replacement
            for (module, name), replacement in self.remapping_map.items()
            if is_spec(replacement) or is_module_type(replacement)
        }

        # after this loop we should have an exact module for every module_ref on every blueprint
        for blueprint in self.blueprints:
            for each_module_ref in blueprint.module_refs:
                # we've got to find a another module that implements this spec
                spec = mod_and_mod_ref_to_proxy.get(
                    (blueprint.module, each_module_ref.name), each_module_ref.spec
                )

                # if the spec is actually module, use that (basically a user override)
                if is_module_type(spec):
                    mod_and_mod_ref_to_proxy[blueprint.module, each_module_ref.name] = spec
                    continue

                # find all available candidates
                possible_module_candidates = [
                    each_other_blueprint.module
                    for each_other_blueprint in self.blueprints
                    if (
                        each_other_blueprint != blueprint
                        and spec_structural_compliance(each_other_blueprint.module, spec)
                    )
                ]
                # we keep valid separate from invalid to provide a better error message for "almost" valid cases
                valid_module_candidates = [
                    each_candidate
                    for each_candidate in possible_module_candidates
                    if spec_annotation_compliance(each_candidate, spec)
                ]
                # none
                if len(possible_module_candidates) == 0:
                    raise Exception(
                        f"""The {blueprint.module.__name__} has a module reference ({each_module_ref}) which requested a module that fills out the {each_module_ref.spec.__name__} spec. But I couldn't find a module that met that spec.\n"""
                    )
                # exactly one structurally valid candidate
                elif len(possible_module_candidates) == 1:
                    if len(valid_module_candidates) == 0:
                        logger.warning(
                            f"""The {blueprint.module.__name__} has a module reference ({each_module_ref}) which requested a module that fills out the {each_module_ref.spec.__name__} spec. I found a module ({possible_module_candidates[0].__name__}) that met that spec structurally, but it had a mismatch in type annotations.\nPlease either change the {each_module_ref.spec.__name__} spec or the {possible_module_candidates[0].__name__} module.\n"""
                        )
                    mod_and_mod_ref_to_proxy[blueprint.module, each_module_ref.name] = (
                        possible_module_candidates[0]
                    )
                    continue
                # more than one
                elif len(valid_module_candidates) > 1:
                    raise Exception(
                        f"""The {blueprint.module.__name__} has a module reference ({each_module_ref}) which requested a module that fills out the {each_module_ref.spec.__name__} spec. But I found multiple modules that met that spec: {possible_module_candidates}.\nTo fix this use .remappings, for example:\n    autoconnect(...).remappings([ ({blueprint.module.__name__}, {each_module_ref.name!r}, <ModuleThatHasTheRpcCalls>) ])\n"""
                    )
                # structural candidates, but no valid candidates
                elif len(valid_module_candidates) == 0:
                    possible_module_candidates_str = ", ".join(
                        [each_candidate.__name__ for each_candidate in possible_module_candidates]
                    )
                    raise Exception(
                        f"""The {blueprint.module.__name__} has a module reference ({each_module_ref}) which requested a module that fills out the {each_module_ref.spec.__name__} spec. Some modules ({possible_module_candidates_str}) met the spec structurally but had a mismatch in type annotations\n"""
                    )
                # one valid candidate (and more than one structurally valid candidate)
                else:
                    mod_and_mod_ref_to_proxy[blueprint.module, each_module_ref.name] = (
                        valid_module_candidates[0]
                    )

        # now that we know the streams, we mutate the RPCClient objects
        for (base_module, module_ref_name), target_module in mod_and_mod_ref_to_proxy.items():
            base_module_proxy = module_coordinator.get_instance(base_module)
            target_module_proxy = module_coordinator.get_instance(target_module)  # type: ignore[type-var,arg-type]
            setattr(
                base_module_proxy,
                module_ref_name,
                target_module_proxy,
            )
            # Ensure the remote module instance can use the module ref inside its own RPC handlers.
            base_module_proxy.set_module_ref(module_ref_name, target_module_proxy)

    def _connect_rpc_methods(self, module_coordinator: ModuleCoordinator) -> None:
        # Gather all RPC methods.
        rpc_methods = {}
        rpc_methods_dot = {}

        # Track interface methods to detect ambiguity.
        interface_methods: defaultdict[str, list[tuple[type[ModuleBase], Callable[..., Any]]]] = (
            defaultdict(list)
        )  # interface_name_method -> [(module_class, method)]
        interface_methods_dot: defaultdict[
            str, list[tuple[type[ModuleBase], Callable[..., Any]]]
        ] = defaultdict(list)  # interface_name.method -> [(module_class, method)]

        for blueprint in self.blueprints:
            for method_name in blueprint.module.rpcs.keys():  # type: ignore[attr-defined]
                module_proxy = module_coordinator.get_instance(blueprint.module)  # type: ignore[assignment]
                method_for_rpc_client = getattr(module_proxy, method_name)
                # Register under concrete class name (backward compatibility)
                rpc_methods[f"{blueprint.module.__name__}_{method_name}"] = method_for_rpc_client
                rpc_methods_dot[f"{blueprint.module.__name__}.{method_name}"] = (
                    method_for_rpc_client
                )

                # Also register under any interface names
                for base in blueprint.module.mro():
                    # Check if this base is an abstract interface with the method
                    if (
                        base is not Module
                        and issubclass(base, ABC)
                        and hasattr(base, method_name)
                        and getattr(base, method_name, None) is not None
                    ):
                        interface_key = f"{base.__name__}.{method_name}"
                        interface_methods_dot[interface_key].append(
                            (blueprint.module, method_for_rpc_client)
                        )
                        interface_key_underscore = f"{base.__name__}_{method_name}"
                        interface_methods[interface_key_underscore].append(
                            (blueprint.module, method_for_rpc_client)
                        )

        # Check for ambiguity in interface methods and add non-ambiguous ones
        for interface_key, implementations in interface_methods_dot.items():
            if len(implementations) == 1:
                rpc_methods_dot[interface_key] = implementations[0][1]
        for interface_key, implementations in interface_methods.items():
            if len(implementations) == 1:
                rpc_methods[interface_key] = implementations[0][1]

        # Fulfil method requests (so modules can call each other).
        for blueprint in self.blueprints:
            instance = module_coordinator.get_instance(blueprint.module)  # type: ignore[assignment]

            for method_name in blueprint.module.rpcs.keys():  # type: ignore[attr-defined]
                if not method_name.startswith("set_"):
                    continue

                linked_name = method_name.removeprefix("set_")

                self._check_ambiguity(linked_name, interface_methods, blueprint.module)

                if linked_name not in rpc_methods:
                    continue

                getattr(instance, method_name)(rpc_methods[linked_name])

            for requested_method_name in instance.get_rpc_method_names():  # type: ignore[union-attr]
                self._check_ambiguity(
                    requested_method_name, interface_methods_dot, blueprint.module
                )

                if requested_method_name not in rpc_methods_dot:
                    continue

                instance.set_rpc_method(  # type: ignore[union-attr]
                    requested_method_name, rpc_methods_dot[requested_method_name]
                )

    def build_graph(self, *, open_in_browser: bool = False) -> "BlueprintGraph":
        """Dry-run of build that constructs a graph of modules and streams.

        Every node is a module, every edge is a stream connection.
        Edges are labelled with stream name (and remapped name if different).
        Detached streams (inputs/outputs with no matching counterpart) are shown
        as stub nodes so you can see what isn't connected.

        Args:
            open_in_browser: If True, renders the graph to an HTML file with an
                embedded Mermaid diagram and opens it in the default browser.

        Returns:
            A BlueprintGraph with nodes, edges, and rendering methods.
        """
        from collections import defaultdict as _defaultdict

        nodes: list[GraphNode] = []
        edges: list[GraphEdge] = []

        # Build module nodes
        module_names: dict[type, str] = {}
        for bp in self.blueprints:
            name = bp.module.__name__
            module_names[bp.module] = name
            nodes.append(GraphNode(id=name, label=name, kind="module"))

        # Collect producers/consumers per (remapped_name, type)
        producers: dict[tuple[str, type], list[tuple[type, str]]] = _defaultdict(list)
        consumers: dict[tuple[str, type], list[tuple[type, str]]] = _defaultdict(list)

        for bp in self.blueprints:
            for conn in bp.streams:
                remapped = self.remapping_map.get((bp.module, conn.name), conn.name)
                if not isinstance(remapped, str):
                    continue
                key = (remapped, conn.type)
                if conn.direction == "out":
                    producers[key].append((bp.module, conn.name))
                else:
                    consumers[key].append((bp.module, conn.name))

        # Create edges for connected streams (have both producer and consumer)
        all_keys = set(producers.keys()) | set(consumers.keys())
        connected_keys: set[tuple[str, type]] = set()

        for key in all_keys:
            remapped_name, stream_type = key
            prods = producers.get(key, [])
            cons = consumers.get(key, [])
            if prods and cons:
                connected_keys.add(key)
                for prod_module, prod_original in prods:
                    for cons_module, cons_original in cons:
                        # Build label showing remapped name and originals if different
                        label_parts = [remapped_name]
                        annotations: list[str] = []
                        if prod_original != remapped_name:
                            annotations.append(f"{module_names[prod_module]}.{prod_original}")
                        if cons_original != remapped_name:
                            annotations.append(f"{module_names[cons_module]}.{cons_original}")
                        type_label = stream_type.__name__
                        label = f"{' | '.join(label_parts)}"
                        if annotations:
                            label += f" ({', '.join(annotations)})"
                        label += f" [{type_label}]"

                        edges.append(GraphEdge(
                            source=module_names[prod_module],
                            target=module_names[cons_module],
                            label=label,
                            stream_name=remapped_name,
                            original_name=prod_original if prod_original != remapped_name else None,
                            remapped_name=remapped_name if prod_original != remapped_name else None,
                            stream_type=type_label,
                            connected=True,
                        ))

        # Create detached nodes/edges for unconnected streams
        detached_counter = 0
        for key in all_keys:
            if key in connected_keys:
                continue
            remapped_name, stream_type = key
            type_label = stream_type.__name__

            prods = producers.get(key, [])
            cons = consumers.get(key, [])

            for prod_module, prod_original in prods:
                detached_counter += 1
                stub_id = f"detached_out_{detached_counter}"
                label_parts = [remapped_name]
                if prod_original != remapped_name:
                    label_parts.append(f"(from {prod_original})")
                stub_label = f"? {' '.join(label_parts)} [{type_label}]"
                nodes.append(GraphNode(id=stub_id, label=stub_label, kind="detached"))
                edges.append(GraphEdge(
                    source=module_names[prod_module],
                    target=stub_id,
                    label=f"{remapped_name} [{type_label}]",
                    stream_name=remapped_name,
                    original_name=prod_original if prod_original != remapped_name else None,
                    remapped_name=remapped_name if prod_original != remapped_name else None,
                    stream_type=type_label,
                    connected=False,
                ))

            for cons_module, cons_original in cons:
                detached_counter += 1
                stub_id = f"detached_in_{detached_counter}"
                label_parts = [remapped_name]
                if cons_original != remapped_name:
                    label_parts.append(f"(from {cons_original})")
                stub_label = f"? {' '.join(label_parts)} [{type_label}]"
                nodes.append(GraphNode(id=stub_id, label=stub_label, kind="detached"))
                edges.append(GraphEdge(
                    source=stub_id,
                    target=module_names[cons_module],
                    label=f"{remapped_name} [{type_label}]",
                    stream_name=remapped_name,
                    original_name=cons_original if cons_original != remapped_name else None,
                    remapped_name=remapped_name if cons_original != remapped_name else None,
                    stream_type=type_label,
                    connected=False,
                ))

        graph = BlueprintGraph(nodes=tuple(nodes), edges=tuple(edges))

        if open_in_browser:
            graph.open_in_browser()

        return graph

    def build(
        self,
        cli_config_overrides: Mapping[str, Any] | None = None,
    ) -> ModuleCoordinator:
        logger.info("Building the blueprint")
        global_config.update(**dict(self.global_config_overrides))
        if cli_config_overrides:
            global_config.update(**dict(cli_config_overrides))

        self._run_configurators()
        self._check_requirements()
        self._verify_no_name_conflicts()

        logger.info("Starting the modules")
        module_coordinator = ModuleCoordinator(cfg=global_config)
        module_coordinator.start()

        # all module constructors are called here (each of them setup their own)
        self._deploy_all_modules(module_coordinator, global_config)
        self._connect_streams(module_coordinator)
        self._connect_rpc_methods(module_coordinator)
        self._connect_module_refs(module_coordinator)

        module_coordinator.start_all_modules()

        return module_coordinator


def autoconnect(*blueprints: Blueprint) -> Blueprint:
    all_blueprints = tuple(_eliminate_duplicates([bp for bs in blueprints for bp in bs.blueprints]))
    all_transports = dict(  # type: ignore[var-annotated]
        reduce(operator.iadd, [list(x.transport_map.items()) for x in blueprints], [])
    )
    all_config_overrides = dict(  # type: ignore[var-annotated]
        reduce(operator.iadd, [list(x.global_config_overrides.items()) for x in blueprints], [])
    )
    all_remappings = dict(  # type: ignore[var-annotated]
        reduce(operator.iadd, [list(x.remapping_map.items()) for x in blueprints], [])
    )
    all_requirement_checks = tuple(check for bs in blueprints for check in bs.requirement_checks)
    all_configurator_checks = tuple(check for bs in blueprints for check in bs.configurator_checks)

    return Blueprint(
        blueprints=all_blueprints,
        transport_map=MappingProxyType(all_transports),
        global_config_overrides=MappingProxyType(all_config_overrides),
        remapping_map=MappingProxyType(all_remappings),
        requirement_checks=all_requirement_checks,
        configurator_checks=all_configurator_checks,
    )


def _eliminate_duplicates(blueprints: list[_BlueprintAtom]) -> list[_BlueprintAtom]:
    # The duplicates are eliminated in reverse so that newer blueprints override older ones.
    seen = set()
    unique_blueprints = []
    for bp in reversed(blueprints):
        if bp.module not in seen:
            seen.add(bp.module)
            unique_blueprints.append(bp)
    return list(reversed(unique_blueprints))
