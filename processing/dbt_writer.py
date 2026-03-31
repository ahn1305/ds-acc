import os


def write_dbt_project(base_path, dbt_models):

    for layer, models in dbt_models.items():

        folder = os.path.join(base_path, "models", layer)
        os.makedirs(folder, exist_ok=True)

        for name, sql in models.items():

            file_path = os.path.join(folder, f"{name}.sql")

            with open(file_path, "w") as f:
                f.write(sql)
