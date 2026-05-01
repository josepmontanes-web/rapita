import json
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

    try:
        dt = pd.to_datetime(value, errors="raise")
        return dt.strftime("%Y%m%d")
    except Exception:
        pass

    text = str(value).strip()

    m = re.match(r"^(\d{4})-\d{2}-\d{2}", text)
    if m:
        return f"{m.group(1)}0000"

    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", text)
    if m:
        d, mth, y = m.groups()
        return f"{y}{int(mth):02d}{int(d):02d}"

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


def build_id(source_id, name):
    source_id = clean(source_id)
    normalized_name = normalize_text(name)

    if source_id and normalized_name:
        return f"{source_id}_{normalized_name}"
    if source_id:
        return source_id
    return normalized_name


def full_name(nom, cognoms):
    nom = clean(nom)
    cognoms = clean(cognoms)
    if nom and cognoms:
        return f"{nom} {cognoms}"
    if nom:
        return nom
    if cognoms:
        return cognoms
    return None


def split_children(value):
    value = clean(value)
    if not value:
        return []

    # separa por coma o punto y coma
    parts = re.split(r"\s*;\s*|\s*,\s*", value)
    results = []

    for p in parts:
        p = p.strip()
        if not p:
            continue

        # quita cosas entre corchetes: [aprox 1805]
        p = re.sub(r"\[.*?\]", "", p).strip()

        # quita lugar entre paréntesis: (Tortosa)
        p = re.sub(r"\(.*?\)", "", p).strip()

        if p:
            results.append(p)

    return results


def spouse_name(row, n):
    nom = clean(row.get(f"NOM_CAS{n}"))
    cog = clean(row.get(f"COG_CAS{n}"))
    if nom and cog:
        return f"{nom} {cog}"
    if nom:
        return nom
    if cog:
        return cog
    return None


def guess_sheet_name(xls):
    priority = ["Muestra_ordenada", "Base_dades", "Sheet1"]
    for name in priority:
        if name in xls.sheet_names:
            return name
    return xls.sheet_names[0]


def main():
    xls = pd.ExcelFile(EXCEL_PATH)
    sheet_name = guess_sheet_name(xls)
    df = pd.read_excel(EXCEL_PATH, sheet_name=sheet_name)

    # limpia JSON anteriores
    for old_file in OUTPUT_DIR.glob("*.json"):
        old_file.unlink()

    index_entries = []

    for _, row in df.iterrows():
      name = clean(row.get("NOM_COMPLET")) or full_name(row.get("NOM"), row.get("COGNOMS"))
        if not name:
            continue

        source_id = clean(row.get("IDPersona"))
        birth_raw = row.get("DATA_NEIX")
        birth_place = clean(row.get("LLOC_NEIX"))
        death_raw = row.get("DATA_MORT")
        death_place = clean(row.get("LLOC_MORT"))
        gender = normalize_gender(row.get("SEXE"))
        occupation = clean(row.get("PROFESSIÓ"))
        notes = clean(row.get("TEXTE"))

       person_id = build_id(source_id, name)

        father_name = full_name(row.get("NOM_PARE"), row.get("COGNOMPARE"))
        mother_name = full_name(row.get("NOM_MARE"), row.get("COGNOMMARE"))

        spouse_names = []
        for n in [1, 2, 3]:
            s = spouse_name(row, n)
            if s:
                spouse_names.append(s)

        children_names = split_children(row.get("FILLS"))

        person = {
            "id": person_id,
            "source_id": source_id,
            "name": name,
            "gender": gender,
            "birth": {
                "date": iso_date(birth_raw),
                "place": birth_place
            },
            "death": {
                "date": iso_date(death_raw),
                "place": death_place
            },
          "status": {
    "alive": False if iso_date(death_raw) else True
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
                "parents_names": [x for x in [father_name, mother_name] if x],
                "spouse_names": spouse_names,
                "children_names": children_names
            },
            "occupation": {
                "title": occupation
            },
            "education": [],
            "contact": {},
            "photo": "",
            "notes": notes,
            "genogram": {
                "nodes": [],
                "links": []
            }
        }

        output_file = OUTPUT_DIR / f"{person_id}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(person, f, ensure_ascii=False, indent=2)

        index_entries.append({
            "id": person_id,
            "name": name,
            "file": f"data/persons/{person_id}.json"
        })

    with open(INDEX_PATH, "w", encoding="utf-8") as f:
        json.dump(index_entries, f, ensure_ascii=False, indent=2)

    print(f"Generados {len(index_entries)} JSONs en {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
