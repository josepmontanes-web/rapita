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


def extract_numeric_source_id(value):
    """
    Extrae el número base de referencias del tipo:
    '4826. Madalena Sancho' -> '4826'
    '4826' -> '4826'
    """
    value = clean(value)
    if not value:
        return None

    text = str(value).strip()

    m = re.match(r"^(\d+)\s*\.", text)
    if m:
        return m.group(1)

    m = re.match(r"^(\d+)$", text)
    if m:
        return m.group(1)

    return None


def build_json_id(source_id_numeric, name):
    source_id_numeric = clean(source_id_numeric)
    normalized_name = normalize_text(name)

    if source_id_numeric and normalized_name:
        return f"{source_id_numeric}_{normalized_name}"
    if source_id_numeric:
        return source_id_numeric
    return normalized_name


def get_first_column_name(df):
    return df.columns[0]


def get_row_ref(row, first_col_name):
    """
    Devuelve la referencia exacta de la primera columna.
    Ejemplo: '4826. Madalena Sancho'
    """
    return clean(row.get(first_col_name))


def pick_person_source_numeric(row, row_ref):
    """
    Prioridad:
    1. número sacado de la primera columna exacta
    2. IDPersona
    """
    source_num = extract_numeric_source_id(row_ref)
    if source_num:
        return source_num

    source_num = extract_numeric_source_id(row.get("IDPersona"))
    if source_num:
        return source_num

    raw = clean(row.get("IDPersona"))
    return raw


def build_name_index(people_rows):
    exact_index = defaultdict(list)
    token_index = defaultdict(list)

    for person in people_rows:
        person_json_id = person["id"]
        name = person["name"]
        norm = normalize_text(name)

        if norm:
            exact_index[norm].append(person_json_id)

        tokens = norm.split("_")
        for size in [1, 2, 3, 4]:
            if len(tokens) >= size:
                key = "_".join(tokens[:size])
                token_index[key].append(person_json_id)

    return exact_index, token_index


def build_row_ref_indexes(people_rows):
    """
    Índices para resolver personas por:
    - referencia exacta de primera columna
    - número base
    """
    by_row_ref = {}
    by_numeric_source = {}
    by_name_norm = defaultdict(list)

    for person in people_rows:
        row_ref = person["row_ref"]
        source_id = person["source_id"]
        name_norm = normalize_text(person["name"])

        if row_ref:
            by_row_ref[row_ref] = person

        if source_id:
            by_numeric_source[str(source_id)] = person

        if name_norm:
            by_name_norm[name_norm].append(person)

    return by_row_ref, by_numeric_source, by_name_norm


def resolve_person_by_any_reference(raw_value, by_row_ref, by_numeric_source):
    """
    Resuelve una persona si la celda contiene:
    - referencia exacta: '4826. Madalena Sancho'
    - número: '4826'
    """
    raw_value = clean(raw_value)
    if not raw_value:
        return None

    if raw_value in by_row_ref:
        return by_row_ref[raw_value]

    numeric = extract_numeric_source_id(raw_value)
    if numeric and numeric in by_numeric_source:
        return by_numeric_source[numeric]

    return None


def resolve_person_by_name(raw_name, by_name_norm):
    raw_name = clean(raw_name)
    if not raw_name:
        return None

    norm = normalize_text(raw_name)
    candidates = by_name_norm.get(norm, [])

    if len(candidates) == 1:
        return candidates[0]

    return None


def resolve_spouse_ids(spouse_names, exact_index, token_index, current_person_id):
    resolved_ids = []

    for spouse in spouse_names:
        norm = normalize_text(spouse)
        candidates = []

        if norm in exact_index:
            candidates = exact_index[norm]

        if not candidates:
            tokens = norm.split("_")
            for size in [4, 3, 2, 1]:
                if len(tokens) >= size:
                    key = "_".join(tokens[:size])
                    if key in token_index:
                        candidates = token_index[key]
                        break

        candidates = [c for c in unique_preserve_order(candidates) if c != current_person_id]

        if len(candidates) == 1:
            resolved_ids.append(candidates[0])

    return unique_preserve_order(resolved_ids)


def extract_birth_place_from_text(text):
    """
    Solo para rellenar si LLOC_NEIX está vacío.
    Patrones típicos:
    - 'Naixement: a Freginals'
    - 'Naixement: el 1760 a Alcanar'
    """
    text = clean(text)
    if not text:
        return None

    patterns = [
        r"Naixement:\s*(?:el\s+\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\s+)?a\s+([A-ZÀ-ÿ][^.,;\n]+)",
        r"Naixement:\s*(?:el\s+\d{4}\s+)?a\s+([A-ZÀ-ÿ][^.,;\n]+)",
    ]

    for pattern in patterns:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            place = clean(m.group(1))
            if place:
                place = re.sub(r"\s+", " ", place).strip(" .,:;")
                return place

    return None


def main():
    xls = pd.ExcelFile(EXCEL_PATH)
    sheet_name = guess_sheet_name(xls)
    df = pd.read_excel(EXCEL_PATH, sheet_name=sheet_name)

    first_col_name = get_first_column_name(df)

    for old_file in OUTPUT_DIR.glob("*.json"):
        old_file.unlink()

    people_rows = []
    children_by_parent_numeric = defaultdict(list)

    # Primera pasada
    for _, row in df.iterrows():
        row_ref = get_row_ref(row, first_col_name)
        name = clean(row.get("NOM_COMPLET")) or full_name(row.get("NOM"), row.get("COGNOMS"))

        if not row_ref and not name:
            continue

        source_id_numeric = pick_person_source_numeric(row, row_ref)

        if not name:
            # Si no hay nombre suelto, intentamos sacarlo de la primera columna
            if row_ref and "." in row_ref:
                maybe_name = row_ref.split(".", 1)[1].strip()
                if maybe_name:
                    name = maybe_name

        if not source_id_numeric or not name:
            continue

        birth_raw = row.get("DATA_NEIX")
        death_raw = row.get("DATA_MORT")
        notes = clean(row.get("TEXTE"))

        person_json_id = build_json_id(source_id_numeric, name)

        father_raw = clean(row.get("IDPare")) or clean(row.get("ID_PARE")) or clean(row.get("PareID"))
        mother_raw = clean(row.get("IDMare")) or clean(row.get("ID_MARE")) or clean(row.get("MareID"))

        father_numeric = extract_numeric_source_id(father_raw)
        mother_numeric = extract_numeric_source_id(mother_raw)

        if father_numeric:
            children_by_parent_numeric[father_numeric].append(source_id_numeric)
        if mother_numeric:
            children_by_parent_numeric[mother_numeric].append(source_id_numeric)

        spouse_names = []
        for n in [1, 2, 3]:
            s = spouse_name(row, n)
            if s:
                spouse_names.append(s)
        spouse_names = unique_preserve_order(spouse_names)

        birth_place = clean(row.get("LLOC_NEIX"))
        if not birth_place:
            birth_place = extract_birth_place_from_text(notes)

        person = {
            "id": person_json_id,
            "source_id": source_id_numeric,
            "row_ref": row_ref,  # referencia exacta de la primera columna
            "name": name,
            "gender": normalize_gender(row.get("SEXE")),
            "birth": {
                "date": iso_date(birth_raw),
                "place": birth_place
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
            "notes": notes,
            "genogram": {
                "nodes": [],
                "links": []
            },
            "_meta": {
                "raw_father_ref": father_raw,
                "raw_mother_ref": mother_raw,
                "raw_father_name": full_name(row.get("NOM_PARE"), row.get("COGNOMPARE")),
                "raw_mother_name": full_name(row.get("NOM_MARE"), row.get("COGNOMMARE"))
            }
        }

        people_rows.append(person)

    # Índices
    exact_name_index, token_name_index = build_name_index(people_rows)
    by_row_ref, by_numeric_source, by_name_norm = build_row_ref_indexes(people_rows)

    # Segunda pasada: resolver relaciones
    for person in people_rows:
        parent_ids = []
        parent_names = []

        # Padre
        father_person = resolve_person_by_any_reference(
            person["_meta"]["raw_father_ref"],
            by_row_ref,
            by_numeric_source
        )

        if not father_person:
            father_person = resolve_person_by_name(
                person["_meta"]["raw_father_name"],
                by_name_norm
            )

        if father_person:
            parent_ids.append(father_person["id"])
            parent_names.append(father_person["name"])
        elif person["_meta"]["raw_father_name"]:
            parent_names.append(person["_meta"]["raw_father_name"])

        # Madre
        mother_person = resolve_person_by_any_reference(
            person["_meta"]["raw_mother_ref"],
            by_row_ref,
            by_numeric_source
        )

        if not mother_person:
            mother_person = resolve_person_by_name(
                person["_meta"]["raw_mother_name"],
                by_name_norm
            )

        if mother_person:
            parent_ids.append(mother_person["id"])
            parent_names.append(mother_person["name"])
        elif person["_meta"]["raw_mother_name"]:
            parent_names.append(person["_meta"]["raw_mother_name"])

        # Hijos
        child_ids = []
        child_names = []

        child_source_ids = children_by_parent_numeric.get(person["source_id"], [])
        for child_source_id in child_source_ids:
            child = by_numeric_source.get(str(child_source_id))
            if child:
                child_ids.append(child["id"])
                child_names.append(child["name"])

        # Cónyuges
        spouse_ids = resolve_spouse_ids(
            person["relations_detail"]["spouse_names"],
            exact_name_index,
            token_name_index,
            person["id"]
        )

        person["family"]["parents"] = unique_preserve_order(parent_ids)
        person["family"]["spouse"] = unique_preserve_order(spouse_ids)
        person["family"]["children"] = unique_preserve_order(child_ids)

        person["relations_detail"]["parents_names"] = unique_preserve_order(parent_names)
        person["relations_detail"]["children_names"] = unique_preserve_order(child_names)

    # Escritura final
    index_entries = []

    for person in people_rows:
        person.pop("_meta", None)

        output_file = OUTPUT_DIR / f"{person['id']}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(person, f, ensure_ascii=False, indent=2)

        index_entries.append({
            "id": person["id"],
            "source_id": person["source_id"],
            "row_ref": person["row_ref"],
            "name": person["name"],
            "file": f"data/persons/{person['id']}.json"
        })

    index_entries.sort(key=lambda x: (str(x.get("source_id") or ""), x["name"].lower()))

    with open(INDEX_PATH, "w", encoding="utf-8") as f:
        json.dump(index_entries, f, ensure_ascii=False, indent=2)

    print(f"Generados {len(index_entries)} JSONs en {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
