import re


class InformaticaLineageEngine:

    def __init__(self, graph):
        self.stages = graph.get("stages", {})
        self.links = graph.get("links", [])

        # Build maps
        self.column_map = self.build_column_map()
        self.stage_upstream_map = self.build_stage_upstream_map()

    # =========================================
    # COLUMN MAP
    # =========================================
    def build_column_map(self):

        column_map = {}

        for link in self.links:

            key = (link["to"], link["to_field"])

            if key not in column_map:
                column_map[key] = []

            column_map[key].append({
                "stage": link["from"],
                "column": link["from_field"],
                "type": link.get("from_type")
            })

        return column_map

    # =========================================
    # STAGE UPSTREAM MAP
    # =========================================
    def build_stage_upstream_map(self):

        upstream_map = {}

        for link in self.links:
            upstream_map.setdefault(link["to"], set()).add(link["from"])

        return upstream_map

    # =========================================
    # EXPRESSION PARSER (IMPROVED)
    # =========================================
    def extract_sources(self, expression):

        if not expression:
            return []

        tokens = re.findall(r'[A-Za-z_][A-Za-z0-9_]*', expression)

        keywords = {
            "IIF", "DECODE", "CASE", "WHEN", "THEN", "ELSE",
            "AND", "OR", "NOT", "NULL",
            "TO_DATE", "SYSDATE", "UPPER", "LOWER",
            "COUNT", "SUM", "AVG", "MIN", "MAX",
            "SETVARIABLE"
        }

        return list(set([t for t in tokens if t.upper() not in keywords]))

    # =========================================
    # 🔥 RECURSIVE LINEAGE RESOLUTION
    # =========================================
    def trace_column(self, stage, column, visited=None):

        if visited is None:
            visited = set()

        key = (stage, column)

        if key in visited:
            return []

        visited.add(key)

        # If direct mapping exists
        if key in self.column_map:

            results = []

            for src in self.column_map[key]:

                src_stage = src["stage"]
                src_col = src["column"]

                # If source is a SOURCE → stop recursion
                if src["type"] and "Source Definition" in src["type"]:
                    results.append(f"{src_stage}.{src_col}")
                else:
                    # Recursive trace
                    upstream = self.trace_column(src_stage, src_col, visited)

                    if upstream:
                        results.extend(upstream)
                    else:
                        results.append(f"{src_stage}.{src_col}")

            return results

        # Fallback → expression-based
        stage_meta = self.stages.get(stage, {})
        outputs = stage_meta.get("outputs", [])

        for col in outputs:
            if col["name"] == column:
                expr_sources = self.extract_sources(col.get("derivation"))

                results = []
                for src_col in expr_sources:
                    upstream = self.trace_column(stage, src_col, visited)
                    if upstream:
                        results.extend(upstream)
                    else:
                        results.append(src_col)

                return results

        return []

    # =========================================
    # BUILD LINEAGE
    # =========================================
    def run(self):

        lineage = []

        for stage_name, stage in self.stages.items():

            for col in stage.get("outputs", []):

                target_col = col.get("name")
                derivation = col.get("derivation")
                complexity = col.get("complexity")

                # 🔥 FULL TRACE
                sources = self.trace_column(stage_name, target_col)

                # Fallback
                if not sources:
                    sources = ["UNKNOWN"]

                sources = list(set(sources))

                lineage.append({
                    "source": ", ".join(sources),
                    "target": f"{stage_name}.{target_col}",
                    "transformation": derivation or "Direct",
                    "stage": stage_name,
                    "complexity": complexity
                })

        return lineage