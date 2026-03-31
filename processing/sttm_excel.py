# import pandas as pd
# import os

# def generate_sttm_excel(file_obj, sttm_data):

#     if not sttm_data:
#         sttm_data = [{
#             "source": "EMPTY",
#             "target": "EMPTY",
#             "transformation": "No mapping"
#         }]

#     df = pd.DataFrame(sttm_data)

#     # Normalize keys
#     df = df.rename(columns={
#         "source": "Source Column",
#         "target": "Target Column",
#         "transformation": "Transformation"
#     })

#     # Ensure columns exist
#     for col in ["Source Column", "Target Column", "Transformation"]:
#         if col not in df.columns:
#             df[col] = ""

#     base = f"media/output/{file_obj.batch.id}/"
#     os.makedirs(base, exist_ok=True)

#     path = base + f"{file_obj.id}_sttm.xlsx"

#     df.to_excel(path, index=False)

#     return path


import pandas as pd
import os


def generate_sttm_excel(file_obj, sttm_data):

    try:
        # =============================
        # 🔥 HARD FIX: normalize input
        # =============================
        if not isinstance(sttm_data, list):
            print("⚠️ STTM is not list → fixing")
            sttm_data = []

        clean_data = []

        for row in sttm_data:

            if not isinstance(row, dict):
                continue

            clean_data.append({
                "Source Column": row.get("source", ""),
                "Target Column": row.get("target", ""),
                "Transformation": row.get("transformation", "")
            })

        # fallback if empty
        if not clean_data:
            clean_data = [{
                "Source Column": "EMPTY",
                "Target Column": "EMPTY",
                "Transformation": "No mapping"
            }]

        df = pd.DataFrame(clean_data)

        # =============================
        # 🔥 ENSURE STRUCTURE
        # =============================
        expected_cols = ["Source Column", "Target Column", "Transformation"]

        for col in expected_cols:
            if col not in df.columns:
                df[col] = ""

        df = df[expected_cols]  # enforce order

        # =============================
        # SAVE
        # =============================
        base = f"media/output/{file_obj.batch.id}/"
        os.makedirs(base, exist_ok=True)

        path = os.path.join(base, f"{file_obj.id}_sttm.xlsx")

        df.to_excel(path, index=False)

        print("✅ STTM EXCEL GENERATED:", path)

        return path

    except Exception as e:
        print("❌ STTM EXCEL ERROR:", str(e))

        # fallback file (never crash pipeline)
        base = f"media/output/{file_obj.batch.id}/"
        os.makedirs(base, exist_ok=True)

        path = os.path.join(base, f"{file_obj.id}_sttm_failed.xlsx")

        df = pd.DataFrame([{
            "Source Column": "ERROR",
            "Target Column": "ERROR",
            "Transformation": str(e)
        }])

        df.to_excel(path, index=False)

        return path
