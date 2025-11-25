import os
import json
import ast
import re
import pandas as pd
from sqlalchemy import create_engine
from dateutil import parser
import hashlib

def name_to_int_id(name, bits=64):
    """Gera um ID inteiro estável a partir do nome usando MD5 (64 bits)."""
    if name is None:
        return None
    s = str(name).strip()
    if s == "":
        return None
    h = hashlib.md5(s.encode('utf-8')).hexdigest()
    return int(h[:16], 16)

def read_raw_csv(path):
    """Lê CSV preservando strings (evitando conversões indesejadas)."""
    return pd.read_csv(path, dtype=str, keep_default_na=False)

def _try_parse_json_like(raw):
    """Tenta converter valores textuais em listas ou dicionários."""
    if raw is None:
        return None
    if isinstance(raw, (list, tuple)):
        return list(raw)
    if isinstance(raw, dict):
        return [raw]
    if isinstance(raw, float) or not isinstance(raw, str):
        return None

    s = raw.strip()
    if s == "" or s.lower() in ("nan", "none", "null") or s in ("[]", "[ ]"):
        return None

    for parser_func in (
        lambda x: json.loads(x),
        lambda x: json.loads(x.replace("'", '"')),
        lambda x: ast.literal_eval(x)
    ):
        try:
            return parser_func(s)
        except Exception:
            pass

    # regex para blocos {..}
    items = []
    for m in re.finditer(r"\{[^}]*\}", s):
        txt = m.group(0)
        try:
            obj = ast.literal_eval(txt)
            if isinstance(obj, dict):
                items.append(obj)
                continue
        except Exception:
            pass
        id_m = re.search(r"id\s*:\s*('?\"?)(?P<id>[^'\",}\]]+)", txt)
        name_m = re.search(r"name\s*:\s*('?\"?)(?P<name>[^'\",}]+)", txt)
        if id_m or name_m:
            items.append({
                'id': id_m.group('id').strip("'\"") if id_m else None,
                'name': name_m.group('name').strip("'\"") if name_m else None
            })
    if items:
        return items

    # Fallback: lista separada por vírgulas
    if ("," in s) and not (s.startswith("[") or s.startswith("{") or ":" in s):
        parts = [p.strip() for p in s.split(",") if p.strip()]
        return parts or None

    return None

def parse_date_safe(x):
    try:
        return parser.parse(x).date()
    except Exception:
        return pd.NaT

def basic_clean(df):
    """Limpeza básica de colunas numéricas, datas e booleanas."""
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]

    # ---------------------------
    # 🔥 CORREÇÃO DA release_date
    # ---------------------------
    if 'release_date' in df.columns:
        # Trata valores vazios e inválidos
        df['release_date'] = df['release_date'].replace(["", " ", "None", "nan"], pd.NA)

        # Converte para datetime64 (agora compatível com .dt.year)
        df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')

    # ---------------------------
    # NUMÉRICOS
    # ---------------------------
    for col in ['budget', 'revenue', 'popularity', 'vote_average',
                'vote_count', 'runtime']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # ---------------------------
    # BOOLEANO
    # ---------------------------
    if 'adult' in df.columns:
        df['adult'] = df['adult'].astype(str).str.lower().isin(
            ['true', '1', 't', 'yes', 'y']
        )

    return df


def explode_json_column(df, col, id_col="id"):
    """
    Explode para colunas que contêm listas em texto, separadas por vírgula.
    Exemplo: 'Action, Adventure' → duas linhas.
    """
    if col not in df.columns:
        return None

    rows = []

    for _, row in df.iterrows():
        film_id = row[id_col]
        raw = row[col]

        if pd.isna(raw) or str(raw).strip() == "":
            continue

        # separa valores por vírgula
        items = [x.strip() for x in str(raw).split(",") if x.strip() != ""]

        for item in items:
            rows.append({
                "film_id": film_id,
                "item_name": item,
                "item_id": None  # não existe id → será gerado depois
            })

    if not rows:
        return None

    return pd.DataFrame(rows)


def save_bronze(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)


def create_mysql_engine(host, port, user, password, db, pool_size=10, max_overflow=20):
    from sqlalchemy import create_engine
    return create_engine(
        f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}",
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True
    )

def write_table_engine(df, table_name, engine, if_exists='append', index=False,
                       chunksize=1000, method='multi', **kwargs):
    """
    Grava DataFrame no banco corrigindo tipos incompatíveis.
    Agora suporta parâmetros opcionais chunksize, method e outros kwargs,
    mantendo compatibilidade com chamadas antigas.
    """

    if df is None or df.empty:
        print(f"[write_table_engine] DataFrame vazio para {table_name}, ignorado.")
        return

    # Tratamento dos tipos
    for col in df.columns:
        if str(df[col].dtype) in ("uint64", "UInt64"):
            df[col] = df[col].astype(str)
        elif "int" in str(df[col].dtype):
            df[col] = df[col].astype("Int64")

    # Escreve na tabela com chunksize e método configuráveis
    df.to_sql(
        table_name,
        engine,
        if_exists=if_exists,
        index=index,
        method=method,
        chunksize=chunksize
    )

    print(f"[write_table_engine] {table_name}: {len(df)} linhas gravadas.")