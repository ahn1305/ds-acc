class InformaticaGraphBuilder:

    def __init__(self, parsed):

        if not isinstance(parsed, dict):
            raise ValueError("Parsed Informatica input must be a dict")

        self.job_name = parsed.get("job_name", "UNKNOWN")
        self.stages = parsed.get("stages", {}) or {}
        self.links = parsed.get("links", []) or {}
        self.instance_groups = parsed.get("instance_groups", {})

    # =========================================
    # BUILD STAGE INPUT MAP
    # =========================================
    def build_stage_inputs(self):

        input_map = {stage: set() for stage in self.stages.keys()}

        for link in self.links:

            src = link.get("from")
            tgt = link.get("to")

            if src and tgt:
                input_map.setdefault(tgt, set()).add(src)

        return {k: list(v) for k, v in input_map.items()}

    # =========================================
    # BUILD COLUMN MAP (ENHANCED)
    # =========================================
    def build_column_map(self):

        column_map = {}

        for link in self.links:

            tgt_stage = link.get("to")
            tgt_col = link.get("to_field")

            src_stage = link.get("from")
            src_col = link.get("from_field")

            if not tgt_stage or not tgt_col:
                continue

            key = f"{tgt_stage}.{tgt_col}"

            column_map.setdefault(key, []).append({
                "stage": src_stage,
                "column": src_col,
                "type": link.get("from_type")  # 🔥 NEW
            })

        return column_map

    # =========================================
    # ATTACH INPUTS
    # =========================================
    def attach_inputs_to_stages(self):

        input_map = self.build_stage_inputs()

        for stage_name, inputs in input_map.items():

            if stage_name not in self.stages:
                continue

            self.stages[stage_name]["inputs"] = inputs

        return self.stages

    # =========================================
    # BUILD GRAPH
    # =========================================
    def build_graph(self):

        graph = {stage: [] for stage in self.stages}

        for link in self.links:

            src = link.get("from")
            tgt = link.get("to")

            if src and tgt:
                graph.setdefault(src, []).append(tgt)

        return graph

    # =========================================
    # BUILD REVERSE GRAPH
    # =========================================
    def build_reverse_graph(self):

        reverse = {stage: [] for stage in self.stages}

        for link in self.links:

            src = link.get("from")
            tgt = link.get("to")

            if src and tgt:
                reverse.setdefault(tgt, []).append(src)

        return reverse

    # =========================================
    # TOPOLOGICAL SORT (IMPROVED)
    # =========================================
    def get_execution_order(self):

        graph = self.build_graph()
        visited = set()
        temp = set()
        order = []

        def dfs(node):

            if node in temp:
                return  # cycle protection

            if node in visited:
                return

            temp.add(node)

            for neighbor in graph.get(node, []):
                dfs(neighbor)

            temp.remove(node)
            visited.add(node)
            order.append(node)

        for node in graph:
            dfs(node)

        return order[::-1]

    # =========================================
    # ENRICH STAGE TYPES (ADVANCED)
    # =========================================
    def enrich_stage_types(self):

        sources = set(self.instance_groups.get("sources", {}).keys())
        targets = set(self.instance_groups.get("targets", {}).keys())

        for stage_name, stage in self.stages.items():

            inputs = stage.get("inputs", [])
            stage_type = (stage.get("type") or "").lower()

            # 🔥 TRUE SOURCE
            if stage_name in sources:
                category = "SOURCE"

            # 🔥 TRUE TARGET
            elif stage_name in targets:
                category = "TARGET"

            # 🔥 Lookup
            elif "lookup" in stage_type or stage.get("lookup_condition"):
                category = "LOOKUP"

            # 🔥 Aggregator
            elif "aggregator" in stage_type:
                category = "AGGREGATE"

            # 🔥 Filter
            elif stage.get("filter_condition"):
                category = "FILTER"

            # 🔥 Join
            elif len(inputs) > 1:
                category = "JOIN"

            else:
                category = "TRANSFORM"

            stage["category"] = category

        return self.stages

    # =========================================
    # ADD STAGE METADATA (🔥 NEW)
    # =========================================
    def enrich_stage_metadata(self):

        for stage_name, stage in self.stages.items():

            stage["has_filter"] = bool(stage.get("filter_condition"))
            stage["has_lookup"] = bool(stage.get("lookup_condition"))
            stage["has_aggregation"] = any(
                f.get("complexity") == "AGGREGATION"
                for f in stage.get("outputs", [])
            )

        return self.stages

    # =========================================
    # BUILD LINEAGE-READY GRAPH (🔥 NEW)
    # =========================================
    def build_lineage_ready_graph(self):

        lineage_graph = []

        for link in self.links:

            lineage_graph.append({
                "from": f"{link.get('from')}.{link.get('from_field')}",
                "to": f"{link.get('to')}.{link.get('to_field')}",
                "type": link.get("from_type")
            })

        return lineage_graph

    # =========================================
    # RUN ALL
    # =========================================
    def run(self):

        # Attach inputs
        stages = self.attach_inputs_to_stages()

        # Enrich types
        stages = self.enrich_stage_types()

        # Add metadata
        stages = self.enrich_stage_metadata()

        # Build structures
        column_map = self.build_column_map()
        graph = self.build_graph()
        reverse_graph = self.build_reverse_graph()
        lineage_graph = self.build_lineage_ready_graph()

        execution_order = self.get_execution_order()

        return {
            "job_name": self.job_name,
            "stages": stages,
            "links": self.links,
            "column_map": column_map,
            "graph": graph,
            "reverse_graph": reverse_graph,
            "lineage_graph": lineage_graph,  # 🔥 NEW
            "execution_order": execution_order
        }