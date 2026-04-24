import pandas as pd

def extract_clientes(path):
    return pd.read_csv(path, sep=";", dtype=str)

def extract_tarjetas(path):
    return pd.read_csv(path, sep=";", dtype=str)