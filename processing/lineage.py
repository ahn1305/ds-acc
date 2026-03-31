import re


class LineageEngine:

    def __init__(self, parsed):
        self.stages = parsed["stages"]
        self.links = parsed["links"]

    def extract_sources(self, derivation):

        if not derivation:
            return []

        tokens = re.findall(r'[A-Za-z_][A-Za-z0-9_]*', derivation)

        keywords = {
            "If", "Then", "Else", "And", "Or",
            "IsNull", "CurrentTime", "DateToString"
        }

        return list(set([t for t in tokens if t not in keywords]))

    def run(self):

        lineage = []

        for stage_name, stage in self.stages.items():

            for col in stage["outputs"]:

                target = col["name"]
                derivation = col.get("derivation")

                if derivation:
                    sources = self.extract_sources(derivation)
                else:
                    sources = [target]  # pass-through

                lineage.append({
                    "source": ", ".join(sources),
                    "target": target,
                    "transformation": derivation or "Direct mapping"
                })

        return lineage
