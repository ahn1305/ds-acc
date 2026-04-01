class InformaticaSTTMGenerator:

    def __init__(self, lineage, graph):
        self.lineage = lineage
        self.graph = graph
        self.stages = graph.get("stages", {})

    # =========================================
    # DETERMINE TYPE (UPGRADED)
    # =========================================
    def determine_type(self, source, target, transformation, stage, complexity):

        stage_info = self.stages.get(stage, {})
        category = stage_info.get("category", "")

        # 🔥 priority: complexity
        if complexity == "SYSTEM":
            return "SYSTEM"

        if complexity == "AGGREGATION":
            return "AGGREGATE"

        if complexity == "LOOKUP":
            return "LOOKUP"

        if complexity == "FILTER":
            return "FILTER"

        if complexity == "CONDITIONAL":
            return "DERIVED"

        # 🔥 stage-level hints
        if category == "JOIN":
            return "JOIN"

        if category == "LOOKUP":
            return "LOOKUP"

        # 🔥 source-based logic
        if not source or source == "SYSTEM":
            return "SYSTEM"

        if "," in source:
            return "DERIVED"

        if source == target and (
            not transformation or transformation.lower() == target.lower()
        ):
            return "DIRECT"

        return "DERIVED"

    # =========================================
    # CLEAN TRANSFORMATION
    # =========================================
    def clean_transformation(self, transformation):

        if not transformation:
            return ""

        t = str(transformation).replace("\\", "").strip()

        if t.lower() in ["", "null", "none"]:
            return ""

        return t

    # =========================================
    # INCOMPLETE DETECTION (ENHANCED)
    # =========================================
    def is_incomplete(self, transformation, source, t_type):

        t = transformation.lower()

        if (
            "\\" in transformation or
            ("then" in t and "else" not in t) or
            ("else" in t and "then" not in t) or
            t.endswith(("=", "then", "else", "and", "or")) or
            t.count("'") % 2 != 0 or
            ("if" in t and "then" not in t) or
            ("," in source and t_type not in ["DERIVED", "JOIN"]) or
            (t_type != "DIRECT" and "mapped directly" in t)
        ):
            return True

        return False

    # =========================================
    # CONFIDENCE (SMARTER)
    # =========================================
    def calculate_confidence(self, t_type, incomplete, source, complexity):

        confidence = 100

        if incomplete:
            confidence -= 40

        if t_type in ["DERIVED", "JOIN"]:
            confidence -= 10

        if t_type == "SYSTEM":
            confidence -= 20

        if complexity in ["CONDITIONAL", "AGGREGATION"]:
            confidence -= 10

        if "," in source:
            confidence -= 10

        return max(confidence, 0)

    # =========================================
    # RUN
    # =========================================
    def run(self):

        sttm = []

        if not self.lineage:
            return [{
                "source": "UNKNOWN",
                "target": "UNKNOWN",
                "transformation": "No lineage found",
                "stage": "UNKNOWN",
                "type": "UNKNOWN",
                "confidence": 0,
                "incomplete": True
            }]

        for row in self.lineage:

            source = row.get("source") or "SYSTEM"
            target = row.get("target")
            transformation = self.clean_transformation(row.get("transformation"))
            stage = row.get("stage", "UNKNOWN")
            complexity = row.get("complexity")

            # -----------------------------
            # TYPE
            # -----------------------------
            t_type = self.determine_type(
                source, target, transformation, stage, complexity
            )

            # -----------------------------
            # DEFAULT TRANSFORMATION
            # -----------------------------
            if not transformation:

                if t_type == "DIRECT":
                    transformation = f"{source} mapped directly"

                elif t_type == "SYSTEM":
                    transformation = f"Generated field ({target})"

                else:
                    transformation = f"{source} → {target}"

            # -----------------------------
            # INCOMPLETE
            # -----------------------------
            incomplete = self.is_incomplete(transformation, source, t_type)

            # -----------------------------
            # CONFIDENCE
            # -----------------------------
            confidence = self.calculate_confidence(
                t_type, incomplete, source, complexity
            )

            # -----------------------------
            # FINAL
            # -----------------------------
            sttm.append({
        "source": source,
        "target": target,
        "transformation": transformation,
        "stage": stage,
        "type": t_type,
        "complexity": complexity or "DIRECT",  # ✅ FIX
        "confidence": confidence,
        "incomplete": incomplete
    })
        return sttm