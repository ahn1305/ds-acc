import re
import xml.etree.ElementTree as ET



class DSXParser:

    def parse(self, file_path):
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()

        return {
            "job_name": self.extract_job_name(content),
            "stages": self.extract_stages(content),
            "links": self.extract_links(content)
        }

    # =========================================
    # JOB NAME
    # =========================================
    def extract_job_name(self, content):
        match = re.search(r'Identifier "(.*?)"', content)
        return match.group(1) if match else "UNKNOWN"

    # =========================================
    # STAGES + COLUMNS
    # =========================================
    def extract_stages(self, content):

        stage_blocks = re.findall(r'BEGIN STAGE(.*?)END STAGE', content, re.DOTALL)

        stages = {}

        for block in stage_blocks:

            name = self._extract(r'Identifier "(.*?)"', block)
            stage_type = self._extract(r'StageType "(.*?)"', block)

            outputs = self.extract_columns(block)
            inputs = self.extract_inputs(block)

            stages[name] = {
                "type": stage_type,
                "inputs": inputs,
                "outputs": outputs
            }

        return stages

    def extract_columns(self, block):

        column_blocks = re.findall(r'BEGIN COLUMN(.*?)END COLUMN', block, re.DOTALL)

        columns = []

        for col in column_blocks:
            name = self._extract(r'Name "(.*?)"', col)
            derivation = self._extract(r'Derivation "(.*?)"', col)

            columns.append({
                "name": name,
                "derivation": derivation
            })

        return columns

    def extract_inputs(self, block):

        input_blocks = re.findall(r'BEGIN INPUT(.*?)END INPUT', block, re.DOTALL)

        inputs = []

        for inp in input_blocks:
            link = self._extract(r'LinkName "(.*?)"', inp)
            inputs.append(link)

        return inputs

    # =========================================
    # LINKS
    # =========================================
    def extract_links(self, content):

        link_blocks = re.findall(r'BEGIN LINK(.*?)END LINK', content, re.DOTALL)

        links = []

        for block in link_blocks:
            from_stage = self._extract(r'FromStage "(.*?)"', block)
            to_stage = self._extract(r'ToStage "(.*?)"', block)

            links.append({
                "from": from_stage,
                "to": to_stage
            })

        return links

    # =========================================
    # HELPER
    # =========================================
    def _extract(self, pattern, text):
        match = re.search(pattern, text)
        return match.group(1) if match else None






class InformaticaParser:

    def parse(self, file_path):
        tree = ET.parse(file_path)
        root = tree.getroot()

        return {
            "job_name": self.extract_job_name(root),
            "stages": self.extract_transformations(root),
            "links": self.extract_connectors(root)
        }

    # =========================================
    # JOB NAME
    # =========================================
    def extract_job_name(self, root):
        mapping = root.find(".//MAPPING")
        return mapping.get("NAME") if mapping is not None else "UNKNOWN"

    # =========================================
    # TRANSFORMATIONS → STAGES
    # =========================================
    def extract_transformations(self, root):

        stages = {}

        for trans in root.findall(".//TRANSFORMATION"):

            name = trans.get("NAME")
            trans_type = trans.get("TYPE")

            inputs = self.extract_inputs(trans)
            outputs = self.extract_fields(trans)

            stages[name] = {
                "type": trans_type,
                "inputs": inputs,
                "outputs": outputs
            }

        return stages

    # =========================================
    # FIELDS (COLUMNS)
    # =========================================
    def extract_fields(self, trans):

        fields = []

        for field in trans.findall(".//TRANSFORMFIELD"):

            name = field.get("NAME")
            expression = field.get("EXPRESSION")

            fields.append({
                "name": name,
                "derivation": expression
            })

        return fields

    # =========================================
    # INPUT LINKS (LOGICAL)
    # =========================================
    def extract_inputs(self, trans):
        # Placeholder — actual input mapping comes from connectors
        return []

    # =========================================
    # CONNECTORS → LINKS
    # =========================================
    def extract_connectors(self, root):

        links = []

        for conn in root.findall(".//CONNECTOR"):

            from_instance = conn.get("FROMINSTANCE")
            to_instance = conn.get("TOINSTANCE")

            links.append({
                "from": from_instance,
                "to": to_instance
            })

        return links