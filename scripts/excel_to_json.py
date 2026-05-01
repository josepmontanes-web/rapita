import json
import os
import re
import unicodedata
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
EXCEL_PATH = ROOT / "data" / "Base_dades.xlsx"
OUTPUT_DIR = ROOT / "data" / "persons"
INDEX_PATH = ROOT / "data" / "index.json"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def clean(value):
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None


def normalize_text(text):
    if text is None:
        return ""
    text = str(text).strip().lower()
    text = "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )
    text = re.sub(r"[^\w\s]", "", text)
    text = re.sub(r"\s+", "_", text)
    return text.strip("_")


def normalize_gender(value):
    value = clean(value)
    if not value:
        return None
    value = value.lower()
    if value == "h":
        return "male"
    if value == "d":
        return "female"
    return None


def normalize_date_for_id(value):
    if pd.isna(value) or value is None:
        return "00000000"

    # datetime / excel date
    try:
        dt = pd.to_datetime(value, errors="raise")
        return dt.strftime("%Y%m%d")
    except Exception:
        pass

    text = str(value).strip()

    # yyyy-mm-dd hh:mm:ss -> coger año
    m = re.match(r"^(\d{4})-\d{2}-\d{2}", text)
    if m:
        return f"{m.group(1)}0000"

    # dd/mm/yyyy
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", text)
    if m:
        d, mth, y = m.groups()
        return f"{y}{int(mth):02d}{int(d):02d}"

    # yyyy
    m = re.match(r"^(\d{4})$", text)
    if m:
        return f"{m.group(1)}0000"

    return "00000000"


def iso_date(value):
    if pd.isna(value) or value is None:
        return None

    try:
        dt = pd.to_datetime(value, errors="raise")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass

    text = str(value).strip()

    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", text)
    if m:
        d, mth, y = m.groups()
        return f"{y}-{int(mth):02d}-{int(d):02d}"

    m = re.match(r"^(\d{4})$", text)
    if m:
        return f"{m.group(1)}-01-01"

    m = re.match(r"^(\d{4})-\d{2}-\d{2}", text)
    if m:
        return text[:10]

    return None


def guess_column(df, candidates, required=True):
    lower_map = {str(c).strip().lower(): c for c in df.columns}
    for candidate in candidates:
        if candidate.lower() in lower_map:
            return lower_map[candidate.lower()]
    if required:
        raise ValueError(f"No encuentro ninguna de estas columnas: {candidates}")
    return None


def build_id(name, date_value):
    return f"{normalize_text(name)}_{normalize_date_for_id(date_value)}"


def split_names(value):
    value = clean(value)
    if not value:
        return []
    parts = re.split(r"\s*\|\s*|\s*;\s*", value)
    return [p.strip() for p in parts if p.strip()]


def main():
    xls = pd.ExcelFile(EXCEL_PATH)

    # Usa la hoja "Muestra_ordenada" si existe. Si no, la primera.
    sheet_name = "Muestra_ordenada" if "Muestra_ordenada" in xls.sheet_names else xls.sheet_names[0]
    df = pd.read_excel(EXCEL_PATH, sheet_name=sheet_name)

    # Ajusta aquí si luego renombras columnas
    col_name = guess_column(df, ["name", "nom_complet", "nom complet", "nom", "NOM_COMPLET"])
    col_gender = guess_column(df, ["gender", "sexe", "sexo"], required=False)
    col_birth_date = guess_column(df, ["birth_date", "data_naixement", "data naixement", "data", "DATA_NAIX", "fecha"], required=False)
    col_birth_place = guess_column(df, ["birth_place", "lloc_naixement", "lloc naixement", "lloc", "LLOC_NAIX"], required=False)
    col_source_id = guess_column(df, ["source_id", "idpersona", "IDPersona", "ETIQUETA_FUENTE"], required=False)
    col_notes = guess_column(df, ["notes", "observacions", "notes_biografiques", "nota"], required=False)
    col_occupation = guess_column(df, ["occupation", "professio", "profesio", "ofici"], required=False)

    # opcionales
    col_parents = guess_column(df, ["parents_names", "pares", "padres"], required=False)
    col_spouses = guess_column(df, ["spouse_names", "conjuges", "conyuges", "spouses"], required=False)
    col_children = guess_column(df, ["children_names", "fills", "hijos", "children"], required=False)

    generated = []
    index_entries = []

    # limpia jsons anteriores para evitar restos viejos
    for old_file in OUTPUT_DIR.glob("*.json"):
        old_file.unlink()

    for _, row in df.iterrows():
        name = clean(row.get(col_name))
        if not name:
            continue

        birth_raw = row.get(col_birth_date) if col_birth_date else None
        person_id = build_id(name, birth_raw)

        person = {
            "id": person_id,
            "source_id": clean(row.get(col_source_id)) if col_source_id else None,
            "name": name,
            "gender": normalize_gender(row.get(col_gender)) if col_gender else None,
            "birth": {
                "date": iso_date(birth_raw),
                "place": clean(row.get(col_birth_place)) if col_birth_place else None
            },
            "status": {
                "alive": False
            },
            "location": {
                "address": "",
                "lat": None,
                "lng": None
            },
            "family": {
                "parents": [],
                "spouse": [],
                "children": []
            },
            "relations_detail": {
                "parents_names": split_names(row.get(col_parents)) if col_parents else [],
                "spouse_names": split_names(row.get(col_spouses)) if col_spouses else [],
                "children_names": split_names(row.get(col_children)) if col_children else []
            },
            "occupation": {
                "title": clean(row.get(col_occupation)) if col_occupation else None
            },
            "education": [],
            "contact": {},
            "photo": "",
            "notes": clean(row.get(col_notes)) if col_notes else None,
            "genogram": {
                "nodes": [],
                "links": []
            }
        }

        output_file = OUTPUT_DIR / f"{person_id}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(person, f, ensure_ascii=False, indent=2)

        generated.append(person)
        index_entries.append({
            "id": person_id,
            "name": name,
            "file": f"data/persons/{person_id}.json"
        })

    with open(INDEX_PATH, "w", encoding="utf-8") as f:
        json.dump(index_entries, f, ensure_ascii=False, indent=2)

    print(f"Generados {len(generated)} JSONs en {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
