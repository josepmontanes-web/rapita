import json
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
EXCEL_PATH = ROOT / "data" / "Base_dades.xlsx"
OUTPUT_DIR = ROOT / "data" / "persons"
INDEX_PATH = ROOT / "data" / "index.json"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def clean(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
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


def iso_date(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

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
    priority = ["Muestra_ordenada", "Base_dades", "Per_cognom", "Sheet1"]
    for name in priority:
        if name in xls.sheet_names:
            return name
    return xls.sheet_names[0]


def unique_preserve_order(values):
    seen = set()
    result = []
    for value in values:
        if not value:
            continue
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


def build_name_index(people_rows):
    """
    Devuelve varios índices de nombre para intentar resolver relaciones.
    """
    exact_index = defaultdict(list)
    loose_index = defaultdict(list)

    for person in people_rows:
        person_id = person["id"]
        name = person["name"]

        exact_index[normalize_text(name)].append(person_id)

        tokens = normalize_text(name).split("_")
        if len(tokens) >= 2:
            loose_index["_".join(tokens[:2])].append(person_id)

        if len(tokens) >= 3:
            loose_index["_".join(tokens[:3])].append(person_id)

    return exact_index, loose_index


def resolve_spouse_ids(spouse_names, exact_index, loose_index, current_person_id):
    """
    Intenta resolver el cónyuge por nombre.
    Solo acepta coincidencia única.
    """
    resolved_ids = []

    for spouse in spouse_names:
        norm = normalize_text(spouse)
        candidates = []

        if norm in exact_index:
            candidates = exact_index[norm]

        if not candidates:
            tokens = norm.split("_")
            if len(tokens) >= 2:
                key2 = "_".join(tokens[:2])
                candidates = loose_index.get(key2, [])

        if not candidates:
            tokens = norm.split("_")
            if len(tokens) >= 1:
                key1 = tokens[0]
                # búsqueda más laxa: primer token incluido al inicio
                partial = []
                for k, ids in loose_index.items():
                    if k.startswith(key1 + "_") or k == key1:
                        partial.extend(ids)
                candidates = partial

        candidates = [c for c in unique_preserve_order(candidates) if c != current_person_id]

        if len(candidates) == 1:
            resolved_ids.append(candidates[0])

    return unique_preserve_order(resolved_ids)


def main():
    xls = pd.ExcelFile(EXCEL_PATH)
    sheet_name = guess_sheet_name(xls)
    df = pd.read_excel(EXCEL_PATH, sheet_name=sheet_name)

    # Limpia JSON anteriores
    for old_file in OUTPUT_DIR.glob("*.json"):
        old_file.unlink()

    people_rows = []
    source_to_person = {}
    children_by_parent_source = defaultdict(list)

    # Primera pasada: crear estructura base e índices
    for _, row in df.iterrows():
        source_id = clean(row.get("IDPersona"))
        name = clean(row.get("NOM_COMPLET")) or full_name(row.get("NOM"), row.get("COGNOMS"))

        if not source_id or not name:
            continue

        birth_raw = row.get("DATA_NEIX")
        death_raw = row.get("DATA_MORT")

        person_id = build_id(source_id, name)

        father_source_id = clean(row.get("IDPare"))
        mother_source_id = clean(row.get("IDMare"))

        if father_source_id:
            children_by_parent_source[father_source_id].append(source_id)
        if mother_source_id:
            children_by_parent_source[mother_source_id].append(source_id)

        spouse_names = []
        for n in [1, 2, 3]:
            s = spouse_name(row, n)
            if s:
                spouse_names.append(s)
        spouse_names = unique_preserve_order(spouse_names)

        person = {
            "id": person_id,
            "source_id": source_id,
            "name": name,
            "gender": normalize_gender(row.get("SEXE")),
            "birth": {
                "date": iso_date(birth_raw),
                "place": clean(row.get("LLOC_NEIX"))
            },
            "death": {
                "date": iso_date(death_raw),
                "place": clean(row.get("LLOC_MORT"))
            },
            "status": {
                "alive": False if iso_date(death_raw) else None
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
                "parents_names": [],
                "spouse_names": spouse_names,
                "children_names": []
            },
            "occupation": {
                "title": clean(row.get("PROFESSIÓ"))
            },
            "education": [],
            "contact": {},
            "photo": "",
            "notes": clean(row.get("TEXTE")),
            "genogram": {
                "nodes": [],
                "links": []
            },
            "_meta": {
                "father_source_id": father_source_id,
                "mother_source_id": mother_source_id,
                "raw_father_name": full_name(row.get("NOM_PARE"), row.get("COGNOMPARE")),
                "raw_mother_name": full_name(row.get("NOM_MARE"), row.get("COGNOMMARE"))
            }
        }

        people_rows.append(person)
        source_to_person[source_id] = person

    # Índices de nombres para resolver cónyuges
    exact_name_index, loose_name_index = build_name_index(people_rows)

    # Segunda pasada: resolver relaciones
    for person in people_rows:
        father_source_id = person["_meta"]["father_source_id"]
        mother_source_id = person["_meta"]["mother_source_id"]

        parent_ids = []
        parent_names = []

        if father_source_id and father_source_id in source_to_person:
            father = source_to_person[father_source_id]
            parent_ids.append(father["id"])
            parent_names.append(father["name"])
        elif person["_meta"]["raw_father_name"]:
            parent_names.append(person["_meta"]["raw_father_name"])

        if mother_source_id and mother_source_id in source_to_person:
            mother = source_to_person[mother_source_id]
            parent_ids.append(mother["id"])
            parent_names.append(mother["name"])
        elif person["_meta"]["raw_mother_name"]:
            parent_names.append(person["_meta"]["raw_mother_name"])

        child_source_ids = children_by_parent_source.get(person["source_id"], [])
        child_ids = []
        child_names = []

        for child_source_id in child_source_ids:
            child = source_to_person.get(child_source_id)
            if child:
                child_ids.append(child["id"])
                child_names.append(child["name"])

        spouse_ids = resolve_spouse_ids(
            person["relations_detail"]["spouse_names"],
            exact_name_index,
            loose_name_index,
            person["id"]
        )

        person["family"]["parents"] = unique_preserve_order(parent_ids)
        person["family"]["spouse"] = unique_preserve_order(spouse_ids)
        person["family"]["children"] = unique_preserve_order(child_ids)

        person["relations_detail"]["parents_names"] = unique_preserve_order(parent_names)
        person["relations_detail"]["children_names"] = unique_preserve_order(child_names)

    # Escribir JSONs finales
    index_entries = []

    for person in people_rows:
        person.pop("_meta", None)

        output_file = OUTPUT_DIR / f"{person['id']}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(person, f, ensure_ascii=False, indent=2)

        index_entries.append({
            "id": person["id"],
            "name": person["name"],
            "file": f"data/persons/{person['id']}.json"
        })

    index_entries.sort(key=lambda x: x["name"].lower())

    with open(INDEX_PATH, "w", encoding="utf-8") as f:
        json.dump(index_entries, f, ensure_ascii=False, indent=2)

    print(f"Generados {len(index_entries)} JSONs en {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
