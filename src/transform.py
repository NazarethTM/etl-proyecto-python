def limpiar_dataframe(df):
    # quitar espacios
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    
    # pasar emails a minúsculas si existe
    if "correo" in df.columns:
        df["correo"] = df["correo"].str.lower()
    
    return df