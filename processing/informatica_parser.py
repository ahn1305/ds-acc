import xml.etree.ElementTree as ET


class InformaticaParser:

    # =========================================
    # MAIN PARSE
    # =========================================
    def parse(self, file_path):

        tree = ET.parse(file_path)
        root = tree.getroot()

        mappings = []

        for mapping in root.findall(".//MAPPING"):

            mapping_data = {
                "name": mapping.get("NAME"),
                "transformations": self.extract_transformations(mapping),
                "connectors": self.extract_connectors(mapping)
            }

            mappings.append(mapping_data)

        # Take first mapping (can extend later)
        mapping = mappings[0] if mappings else {}

        # Extract instances
        instances = self.extract_instances(root)

        # Classify instances (🔥 NEW)
        classified = self.classify_instances(instances)

        parsed = {
            "job_name": mapping.get("name", "UNKNOWN"),
            "stages": self.normalize_stages(mapping),
            "links": self.normalize_links(mapping, instances),
            "instance_groups": classified
        }

        # 🔥 STTM OUTPUT
        parsed["sttm"] = self.generate_sttm(parsed)

        return parsed

    # =========================================
    # EXTRACT INSTANCES
    # =========================================
    def extract_instances(self, root):

        instances = {}

        for inst in root.findall(".//INSTANCE"):
            instances[inst.get("NAME")] = {
                "transformation": inst.get("TRANSFORMATION_NAME"),
                "type": inst.get("TRANSFORMATION_TYPE")
            }

        return instances

    # =========================================
    # CLASSIFY INSTANCES (🔥 NEW)
    # =========================================
    def classify_instances(self, instances):

        classified = {
            "sources": {},
            "targets": {},
            "transformations": {}
        }

        for name, meta in instances.items():

            t_type = meta.get("type", "")

            if "Source Definition" in t_type:
                classified["sources"][name] = meta

            elif "Target Definition" in t_type:
                classified["targets"][name] = meta

            else:
                classified["transformations"][name] = meta

        return classified

    # =========================================
    # TRANSFORMATIONS
    # =========================================
    def extract_transformations(self, mapping):

        transformations = {}

        for t in mapping.findall(".//TRANSFORMATION"):

            name = t.get("NAME")
            t_type = t.get("TYPE")

            fields = []

            for f in t.findall(".//TRANSFORMFIELD"):

                expr = self.clean_expression(f.get("EXPRESSION"))

                fields.append({
                    "name": f.get("NAME"),
                    "expression": expr,
                    "port_type": f.get("PORTTYPE"),
                    "datatype": f.get("DATATYPE"),
                    "complexity": self.detect_complexity(expr)
                })

            # 🔥 Extract TABLEATTRIBUTES properly
            attributes = {}
            for attr in t.findall(".//TABLEATTRIBUTE"):
                attributes[attr.get("NAME")] = attr.get("VALUE")

            transformations[name] = {
                "type": t_type,
                "fields": fields,
                "attributes": attributes,
                "filter_condition": attributes.get("Filter Condition"),
                "lookup_condition": attributes.get("Lookup condition"),
                "lookup_table": attributes.get("Lookup table name")
            }

        return transformations

    # =========================================
    # CONNECTORS
    # =========================================
    def extract_connectors(self, mapping):

        connectors = []

        for c in mapping.findall(".//CONNECTOR"):

            connectors.append({
                "from_instance": c.get("FROMINSTANCE"),
                "from_field": c.get("FROMFIELD"),
                "to_instance": c.get("TOINSTANCE"),
                "to_field": c.get("TOFIELD")
            })

        return connectors

    # =========================================
    # NORMALIZE STAGES
    # =========================================
    def normalize_stages(self, mapping):

        stages = {}

        for name, t in mapping.get("transformations", {}).items():

            inputs = []
            outputs = []
            variables = []

            for f in t.get("fields", []):

                field = {
                    "name": f["name"],
                    "derivation": f.get("expression"),
                    "complexity": f.get("complexity")
                }

                port_type = f.get("port_type")

                if port_type == "INPUT":
                    inputs.append(field)
                elif port_type == "OUTPUT":
                    outputs.append(field)
                else:
                    variables.append(field)

            stages[name] = {
                "type": t.get("type"),
                "inputs": inputs,
                "outputs": outputs,
                "variables": variables,
                "filter_condition": t.get("filter_condition"),
                "lookup_condition": t.get("lookup_condition"),
                "lookup_table": t.get("lookup_table")
            }

        return stages

    # =========================================
    # NORMALIZE LINKS
    # =========================================
    def normalize_links(self, mapping, instances):

        links = []

        for c in mapping.get("connectors", []):

            from_inst = c.get("from_instance")
            to_inst = c.get("to_instance")

            from_meta = instances.get(from_inst, {})
            to_meta = instances.get(to_inst, {})

            from_stage = from_meta.get("transformation", from_inst)
            to_stage = to_meta.get("transformation", to_inst)

            links.append({
                "from": from_stage,
                "to": to_stage,
                "from_field": c.get("from_field"),
                "to_field": c.get("to_field"),
                "from_type": from_meta.get("type"),
                "to_type": to_meta.get("type")
            })

        return links

    # =========================================
    # STTM GENERATION (🔥 CORE FEATURE)
    # =========================================
    def generate_sttm(self, parsed):

        sttm = []

        for link in parsed.get("links", []):

            sttm.append({
                "source_stage": link["from"],
                "target_stage": link["to"],
                "source_field": link["from_field"],
                "target_field": link["to_field"],
                "source_type": link.get("from_type"),
                "target_type": link.get("to_type")
            })

        return sttm

    # =========================================
    # CLEAN EXPRESSION
    # =========================================
    def clean_expression(self, expr):

        if not expr:
            return None

        return expr.replace("\n", " ").replace("\\", "").strip()

    # =========================================
    # COMPLEXITY DETECTION
    # =========================================
    def detect_complexity(self, expr):

        if not expr:
            return "DIRECT"

        e = expr.lower()

        if "iif(" in e or "decode(" in e:
            return "CONDITIONAL"

        if any(x in e for x in ["count(", "sum(", "avg(", "min(", "max("]):
            return "AGGREGATION"

        if "setvariable" in e or "$pm" in e:
            return "SYSTEM"

        if "lookup" in e or "lkp_" in e:
            return "LOOKUP"

        if "||" in e:
            return "CONCAT"

        if "=" in e and "==" not in e:
            return "FILTER"

        return "DERIVED"