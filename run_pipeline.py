from src.extract import extract_clientes, extract_tarjetas
from src.transform import limpiar_dataframe

# rutas
clientes_path = "input/clientes-2025-11-10.csv"
tarjetas_path = "input/Tarjetas-2025-11-10.csv"

# EXTRACT
df_clientes = extract_clientes(clientes_path)
df_tarjetas = extract_tarjetas(tarjetas_path)

# TRANSFORM
df_clientes_clean = limpiar_dataframe(df_clientes)
df_tarjetas_clean = limpiar_dataframe(df_tarjetas)

# SAVE
df_clientes_clean.to_csv("output/clientes_clean.csv", sep=";", index=False)
df_tarjetas_clean.to_csv("output/tarjetas_clean.csv", sep=";", index=False)

print("Pipeline ejecutado correctamente")