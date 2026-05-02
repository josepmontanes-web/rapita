import json
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = ROOT / "data" / "Cens CSV.csv"
OUTPUT_DIR = ROOT / "data" / "persons"
INDEX_PATH = ROOT / "data" / "index.json"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# LIMPIEZA Y NORMALIZACIÓN
# =========================

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
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", "_", text)
    return text.strip("_")


def normalize_header(text):
    return normalize_text(text)


def normalize_gender(value):
    value = clean(value)
    if not value:
        return None

    value = value.lower().strip()

    if value in ["h", "home", "hom", "male", "m"]:
        return "male"

    if value in ["d", "dona", "female", "f"]:
        return "female"

    return None


def iso_date(value):
    value = clean(value)
    if not value:
        return None

    try:
        dt = pd.to_datetime(value, errors="raise", dayfirst=True)
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


def unique_preserve_order(values):
    seen = set()
    result = []

    for value in values:
        value = clean(value)
        if not value:
            continue

        if value not in seen:
            seen.add(value)
            result.append(value)

    return result


# =========================
# COLUMNAS CSV
# =========================

COLUMN_ALIASES = {
    "row_ref": [
        "ID", "id", "IDPersona", "persona", "Persona"
    ],
    "nom": [
        "NOM", "Nom", "nombre", "Nombre"
    ],
    "cognoms": [
        "COGNOMS", "Cognoms", "Apellidos", "apellidos", "cognom"
    ],
    "nom_complet": [
        "NOM_COMPLET", "Nom complet", "Nombre completo"
    ],
    "data_neix": [
        "DATA_NEIX", "Data naixement", "Fecha nacimiento", "Any naix", "Any_naix"
    ],
    "lloc_neix": [
        "LLOC_NEIX", "Lugar Nacimiento", "Lloc naixement", "Lugar nacimiento"
    ],
    "sexe": [
        "SEXE", "Sexo", "Sexe"
    ],
    "data_mort": [
        "MORT", "DATA_MORT", "Data mort", "Fecha muerte"
    ],
    "lloc_mort": [
        "LLOC_MORT", "LLOC", "Lugar muerte", "Lloc mort"
    ],
    "professio": [
        "PROFESSIÓ", "PROFESSIO", "Professió", "Professio", "T4PROFESSI"
    ],
    "texte": [
        "TEXTE", "Texto", "Text", "OBSERVAC_1", "Observacions"
    ],

    # Padres
    "id_pare": [
        "ID Padre", "ID Pare", "IDPare", "ID_PARE", "PareID"
    ],
    "nom_pare": [
        "NOM_PARE", "Nom pare"
    ],
    "cognom_pare": [
        "COGNOMPARE", "COGNOM_PARE", "Cognom pare"
    ],
    "id_mare": [
        "ID Madre", "ID Mare", "IDMare", "ID_MARE", "MareID", "Madr"
    ],
    "nom_mare": [
        "NOM_MARE", "Nom mare"
    ],
    "cognom_mare": [
        "COGNOMMARE", "COGNOM_MARE", "Cognom mare"
    ],

    # Abuelos
    "id_avi_patern": [
        "ID Abuelo paterno", "ID AVI PATERN", "ID_AVI_PATERN", "DAVI_PATERN"
    ],
    "id_avia_paterna": [
        "ID Abuela paterna", "ID AVIA PATERNA", "ID_AVIA_PATERNA"
    ],
    "id_avi_matern": [
        "ID Abuelo materno", "ID AVI MATERN", "ID_AVI_MATERN"
    ],
    "id_avia_materna": [
        "ID Abuela materna", "ID AVIA MATERNA", "ID_AVIA_MATERNA"
    ],

    # Padrí / padrina
    "id_padri": [
        "ID PADRI", "ID Padrí", "ID Padri", "ID_PADRI"
    ],
    "nom_padri": [
        "NOM_PADRI", "Nom padrí", "Nom padri"
    ],
    "cognom_padri": [
        "COGNOM_PADRI", "COGN_PADRI", "COGN"
    ],
    "id_padrina": [
        "ID PADRINA", "ID Padrina", "ID_P切DRINA", "ID_DRINA", "ID DRINA"
    ],
    "nom_padrina": [
        "NOM_PADRINA", "Nom padrina"
    ],
    "cognom_padrina": [
        "COGNOM_PADRINA", "COGN_PADRINA"
    ],

    # Cónyuges
    "nom_cas1": ["NOM_CAS1", "CAS1", "Nom cas1"],
    "cog_cas1": ["COG_CAS1", "COG CAS1", "Cognom cas1"],
    "nom_cas2": ["NOM_CAS2", "CAS2", "Nom cas2"],
    "cog_cas2": ["COG_CAS2", "COG CAS2", "Cognom cas2"],
    "nom_cas3": ["NOM_CAS3", "CAS3", "Nom cas3"],
    "cog_cas3": ["COG_CAS3", "COG CAS3", "Cognom cas3"],

    # Hijos
    "hijos": [
        "Hijos", "Fills", "Fills:", "Fills::", "Hijos:"
    ]
}


def build_column_map(df):
    normalized_columns = {
        normalize_header(col): col
        for col in df.columns
    }

    column_map = {}

    for key, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            norm_alias = normalize_header(alias)
            if norm_alias in normalized_columns:
                column_map[key] = normalized_columns[norm_alias]
                break

    return column_map


def row_get(row, column_map, key):
    col = column_map.get(key)
    if not col:
        return None
    return clean(row.get(col))


# =========================
# IDS
# =========================

def extract_numeric_source_id(value):
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

    m = re.search(r"\b(\d{1,6})\b", text)
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


def extract_name_from_row_ref(row_ref):
    row_ref = clean(row_ref)
    if not row_ref:
        return None

    if "." in row_ref:
        return clean(row_ref.split(".", 1)[1])

    return None


def resolve_ref(raw_value, by_row_ref, by_numeric_source):
    raw_value = clean(raw_value)
    if not raw_value:
        return None

    if raw_value in by_row_ref:
        return by_row_ref[raw_value]

    numeric = extract_numeric_source_id(raw_value)
    if numeric and numeric in by_numeric_source:
        return by_numeric_source[numeric]

    return None


def resolve_by_name(raw_name, by_name_norm):
    raw_name = clean(raw_name)
    if not raw_name:
        return None

    norm = normalize_text(raw_name)
    candidates = by_name_norm.get(norm, [])

    if len(candidates) == 1:
        return candidates[0]

    return None


# =========================
# TEXTO / HIJOS
# =========================

def extract_birth_place_from_text(text):
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
                return re.sub(r"\s+", " ", place).strip(" .,:;")

    return None


def parse_children_refs(value):
    value = clean(value)
    if not value:
        return []

    refs = []

    for match in re.finditer(r"\b(\d{1,6})\s*\.", value):
        refs.append(match.group(1))

    if refs:
        return unique_preserve_order(refs)

    for match in re.finditer(r"\b(\d{1,6})\b", value):
        refs.append(match.group(1))

    return unique_preserve_order(refs)


def spouse_name(row, column_map, n):
    nom = row_get(row, column_map, f"nom_cas{n}")
    cog = row_get(row, column_map, f"cog_cas{n}")
    return full_name(nom, cog)


def resolve_spouse_ids(spouse_names, by_name_norm, current_person_id):
    resolved = []

    for spouse in spouse_names:
        person = resolve_by_name(spouse, by_name_norm)
        if person and person["id"] != current_person_id:
            resolved.append(person["id"])

    return unique_preserve_order(resolved)


# =========================
# ÍNDICES INTERNOS
# =========================

def build_indexes(people_rows):
    by_row_ref = {}
    by_numeric_source = {}
    by_name_norm = defaultdict(list)

    for person in people_rows:
        if person["row_ref"]:
            by_row_ref[person["row_ref"]] = person

        if person["source_id"]:
            by_numeric_source[str(person["source_id"])] = person

        norm = normalize_text(person["name"])
        if norm:
            by_name_norm[norm].append(person)

    return by_row_ref, by_numeric_source, by_name_norm


# =========================
# MAIN
# =========================

def main():
    df = pd.read_csv(
        CSV_PATH,
        dtype=str,
        keep_default_na=False,
        sep=None,
        engine="python"
    )

    df.columns = [str(c).strip() for c in df.columns]

    column_map = build_column_map(df)

    for old_file in OUTPUT_DIR.glob("*.json"):
        old_file.unlink()

    people_rows = []
    children_by_parent_numeric = defaultdict(list)
    explicit_children_by_person_numeric = defaultdict(list)

    # Primera pasada: crear personas
    for _, row in df.iterrows():
        row_ref = row_get(row, column_map, "row_ref")

        name = (
            row_get(row, column_map, "nom_complet")
            or full_name(
                row_get(row, column_map, "nom"),
                row_get(row, column_map, "cognoms")
            )
            or extract_name_from_row_ref(row_ref)
        )

        source_id_numeric = extract_numeric_source_id(row_ref)

        if not source_id_numeric or not name:
            continue

        notes = row_get(row, column_map, "texte")

        birth_place = row_get(row, column_map, "lloc_neix")
        if not birth_place:
            birth_place = extract_birth_place_from_text(notes)

        person_json_id = build_json_id(source_id_numeric, name)

        father_ref = row_get(row, column_map, "id_pare")
        mother_ref = row_get(row, column_map, "id_mare")

        father_numeric = extract_numeric_source_id(father_ref)
        mother_numeric = extract_numeric_source_id(mother_ref)

        if father_numeric:
            children_by_parent_numeric[father_numeric].append(source_id_numeric)

        if mother_numeric:
            children_by_parent_numeric[mother_numeric].append(source_id_numeric)

        explicit_children = parse_children_refs(row_get(row, column_map, "hijos"))
        if explicit_children:
            explicit_children_by_person_numeric[source_id_numeric].extend(explicit_children)

        spouse_names = []
        for n in [1, 2, 3]:
            s = spouse_name(row, column_map, n)
            if s:
                spouse_names.append(s)

        person = {
            "id": person_json_id,
            "source_id": source_id_numeric,
            "row_ref": row_ref,
            "name": name,
            "gender": normalize_gender(row_get(row, column_map, "sexe")),

            "birth": {
                "date": iso_date(row_get(row, column_map, "data_neix")),
                "place": birth_place
            },

            "death": {
                "date": iso_date(row_get(row, column_map, "data_mort")),
                "place": row_get(row, column_map, "lloc_mort")
            },

            "status": {
                "alive": False if iso_date(row_get(row, column_map, "data_mort")) else None
            },

            "location": {
                "address": "",
                "lat": None,
                "lng": None
            },

            "family": {
                "parents": [],
                "father": None,
                "mother": None,
                "spouse": [],
                "children": [],
                "grandparents": {
                    "paternal_grandfather": None,
                    "paternal_grandmother": None,
                    "maternal_grandfather": None,
                    "maternal_grandmother": None
                },
                "godparents": {
                    "godfather": None,
                    "godmother": None
                }
            },

            "relations_detail": {
                "parents_names": [],
                "father_name": full_name(
                    row_get(row, column_map, "nom_pare"),
                    row_get(row, column_map, "cognom_pare")
                ),
                "mother_name": full_name(
                    row_get(row, column_map, "nom_mare"),
                    row_get(row, column_map, "cognom_mare")
                ),
                "spouse_names": unique_preserve_order(spouse_names),
                "children_names": [],
                "grandparents_names": {
                    "paternal_grandfather": "",
                    "paternal_grandmother": "",
                    "maternal_grandfather": "",
                    "maternal_grandmother": ""
                },
                "godparents_names": {
                    "godfather": full_name(
                        row_get(row, column_map, "nom_padri"),
                        row_get(row, column_map, "cognom_padri")
                    ),
                    "godmother": full_name(
                        row_get(row, column_map, "nom_padrina"),
                        row_get(row, column_map, "cognom_padrina")
                    )
                }
            },

            "occupation": {
                "title": row_get(row, column_map, "professio")
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
                "raw_father_ref": father_ref,
                "raw_mother_ref": mother_ref,
                "raw_father_name": full_name(
                    row_get(row, column_map, "nom_pare"),
                    row_get(row, column_map, "cognom_pare")
                ),
                "raw_mother_name": full_name(
                    row_get(row, column_map, "nom_mare"),
                    row_get(row, column_map, "cognom_mare")
                ),
                "raw_paternal_grandfather_ref": row_get(row, column_map, "id_avi_patern"),
                "raw_paternal_grandmother_ref": row_get(row, column_map, "id_avia_paterna"),
                "raw_maternal_grandfather_ref": row_get(row, column_map, "id_avi_matern"),
                "raw_maternal_grandmother_ref": row_get(row, column_map, "id_avia_materna"),
                "raw_godfather_ref": row_get(row, column_map, "id_padri"),
                "raw_godmother_ref": row_get(row, column_map, "id_padrina")
            }
        }

        people_rows.append(person)

    by_row_ref, by_numeric_source, by_name_norm = build_indexes(people_rows)

    # Segunda pasada: resolver relaciones
    for person in people_rows:
        parent_ids = []
        parent_names = []

        father = resolve_ref(
            person["_meta"]["raw_father_ref"],
            by_row_ref,
            by_numeric_source
        ) or resolve_by_name(
            person["_meta"]["raw_father_name"],
            by_name_norm
        )

        mother = resolve_ref(
            person["_meta"]["raw_mother_ref"],
            by_row_ref,
            by_numeric_source
        ) or resolve_by_name(
            person["_meta"]["raw_mother_name"],
            by_name_norm
        )

        if father:
            person["family"]["father"] = father["id"]
            parent_ids.append(father["id"])
            parent_names.append(father["name"])
        elif person["_meta"]["raw_father_name"]:
            parent_names.append(person["_meta"]["raw_father_name"])

        if mother:
            person["family"]["mother"] = mother["id"]
            parent_ids.append(mother["id"])
            parent_names.append(mother["name"])
        elif person["_meta"]["raw_mother_name"]:
            parent_names.append(person["_meta"]["raw_mother_name"])

        person["family"]["parents"] = unique_preserve_order(parent_ids)
        person["relations_detail"]["parents_names"] = unique_preserve_order(parent_names)

        # Abuelos
        grandparent_map = {
            "paternal_grandfather": "raw_paternal_grandfather_ref",
            "paternal_grandmother": "raw_paternal_grandmother_ref",
            "maternal_grandfather": "raw_maternal_grandfather_ref",
            "maternal_grandmother": "raw_maternal_grandmother_ref"
        }

        for target_key, meta_key in grandparent_map.items():
            gp = resolve_ref(
                person["_meta"].get(meta_key),
                by_row_ref,
                by_numeric_source
            )

            if gp:
                person["family"]["grandparents"][target_key] = gp["id"]
                person["relations_detail"]["grandparents_names"][target_key] = gp["name"]

        # Padrí / padrina
        godfather = resolve_ref(
            person["_meta"]["raw_godfather_ref"],
            by_row_ref,
            by_numeric_source
        )

        godmother = resolve_ref(
            person["_meta"]["raw_godmother_ref"],
            by_row_ref,
            by_numeric_source
        )

        if godfather:
            person["family"]["godparents"]["godfather"] = godfather["id"]
            person["relations_detail"]["godparents_names"]["godfather"] = godfather["name"]

        if godmother:
            person["family"]["godparents"]["godmother"] = godmother["id"]
            person["relations_detail"]["godparents_names"]["godmother"] = godmother["name"]

        # Hijos: desde columnas de padres + columna Hijos
        child_ids = []
        child_names = []

        inferred_child_source_ids = children_by_parent_numeric.get(person["source_id"], [])
        explicit_child_source_ids = explicit_children_by_person_numeric.get(person["source_id"], [])

        all_child_source_ids = unique_preserve_order(
            inferred_child_source_ids + explicit_child_source_ids
        )

        for child_source_id in all_child_source_ids:
            child = by_numeric_source.get(str(child_source_id))
            if child:
                child_ids.append(child["id"])
                child_names.append(child["name"])

        person["family"]["children"] = unique_preserve_order(child_ids)
        person["relations_detail"]["children_names"] = unique_preserve_order(child_names)

        # Cónyuges
        person["family"]["spouse"] = resolve_spouse_ids(
            person["relations_detail"]["spouse_names"],
            by_name_norm,
            person["id"]
        )

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

    index_entries.sort(
        key=lambda x: (
            int(x["source_id"]) if str(x["source_id"]).isdigit() else 999999,
            x["name"].lower()
        )
    )

    with open(INDEX_PATH, "w", encoding="utf-8") as f:
        json.dump(index_entries, f, ensure_ascii=False, indent=2)

    print(f"Generados {len(index_entries)} JSONs en {OUTPUT_DIR}")
    print(f"Índice generado en {INDEX_PATH}")


if __name__ == "__main__":
    main()
