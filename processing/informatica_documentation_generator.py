import json


class InformaticaDocumentationGenerator:

    def __init__(self, parsed, sttm, sql_models, llm_client=None):
        self.parsed = parsed
        self.sttm = sttm
        self.sql_models = sql_models
        self.llm_client = llm_client  # optional (plug later)

    # =========================================
    # DETECT SOURCE STAGES
    # =========================================
    def get_sources(self):

        sources = []

        for stage_name, stage in self.parsed.get("stages", {}).items():

            if not stage.get("inputs"):
                sources.append(stage_name)

        return sources

    # =========================================
    # DETECT FINAL TARGETS
    # =========================================
    def get_targets(self):

        targets = []

        for stage_name, stage in self.parsed.get("stages", {}).items():

            if stage.get("type") == "Target Definition":
                targets.append(stage_name)

        return targets

    # =========================================
    # GROUP TRANSFORMATIONS
    # =========================================
    def summarize_transformations(self):

        summary = {}

        for row in self.sttm:

            stage = row.get("stage")

            if stage not in summary:
                summary[stage] = []

            summary[stage].append(row)

        return summary

    # =========================================
    # BASIC DOCUMENTATION (NO LLM)
    # =========================================
    def generate_basic_doc(self):

        sources = self.get_sources()
        targets = self.get_targets()
        transformations = self.summarize_transformations()

        doc = []

        # -------------------------
        # OVERVIEW
        # -------------------------
        doc.append("📌 OVERVIEW\n")
        doc.append(f"Pipeline Name: {self.parsed.get('job_name')}\n")

        # -------------------------
        # SOURCES
        # -------------------------
        doc.append("\n📥 SOURCE SYSTEMS\n")
        for s in sources:
            doc.append(f"- {s}")

        # -------------------------
        # TRANSFORMATIONS
        # -------------------------
        doc.append("\n🔄 TRANSFORMATIONS\n")

        for stage, rows in transformations.items():

            doc.append(f"\n➡ Stage: {stage}")

            for r in rows[:5]:  # limit for readability
                doc.append(
                    f"  - {r['source']} → {r['target']} ({r['type']})"
                )

        # -------------------------
        # TARGETS
        # -------------------------
        doc.append("\n📤 TARGETS\n")
        for t in targets:
            doc.append(f"- {t}")

        # -------------------------
        # SQL MODELS
        # -------------------------
        doc.append("\n❄️ DBT MODELS GENERATED\n")

        for layer, models in self.sql_models.items():
            doc.append(f"\n{layer.upper()}:")

            for model in models.keys():
                doc.append(f"  - {model}")

        return "\n".join(doc)

    # =========================================
    # LLM DOCUMENTATION (OPTIONAL)
    # =========================================
    def generate_llm_doc(self):

        if not self.llm_client:
            return self.generate_basic_doc()

        try:
            prompt = f"""
You are a senior data engineer.

Explain this ETL pipeline clearly.

Include:
1. Overview
2. Sources
3. Transformations
4. Business Logic
5. Output tables

STTM:
{json.dumps(self.sttm[:10], indent=2)}

SQL:
{json.dumps(self.sql_models, indent=2)[:1500]}
"""

            response = self.llm_client.models.generate_content(
                model="gemini-3.1-flash-lite-preview",
                contents=prompt
            )

            return self.clean_output(response.text)

        except Exception:
            return self.generate_basic_doc()

    # =========================================
    # CLEAN OUTPUT
    # =========================================
    def clean_output(self, text):

        if "```" in text:
            parts = text.split("```")
            text = parts[1] if len(parts) > 1 else parts[0]

        return text.replace("markdown", "").strip()

    # =========================================
    # RUN
    # =========================================
    def run(self):

        return self.generate_llm_doc()