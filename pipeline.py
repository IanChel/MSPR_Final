"""
pipeline.py — Electio-Analytics MSPR Big Data
ETL complet : chargement, nettoyage, normalisation, filtrage Nouvelle-Aquitaine
Périmètre géographique : Nouvelle-Aquitaine (code région INSEE = 75)

Auteurs : Groupe 1/2
"""

import os
import re
import unicodedata
import zipfile
import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION GLOBALE
# ─────────────────────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Code INSEE de la région Nouvelle-Aquitaine (après réforme 2016)
CODE_REGION_NA = 75

# Codes des 12 départements de Nouvelle-Aquitaine (format string 2 chiffres)
DEPTS_NA = {"16", "17", "19", "23", "24", "33", "40", "47", "64", "79", "86", "87"}

# Variantes d'écriture du nom de région pour le filtrage textuel
REGION_ALIASES_NA = [
    "nouvelle-aquitaine",
    "nouvelle aquitaine",
    "nouvelleaquitaine",
    "nlle-aquitaine",
    "nlle aquitaine",
]

# Répertoire de sortie
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Référentiel CODGEO Nouvelle-Aquitaine (construit dynamiquement)
# Contient l'ensemble des codes communes INSEE à 5 caractères de la NA
CODGEO_NA_REF: set = set()


# ─────────────────────────────────────────────────────────────────────────────
# UTILITAIRES TEXTE
# ─────────────────────────────────────────────────────────────────────────────

def remove_accents(text: str) -> str:
    """Supprime les accents d'une chaîne Unicode."""
    return "".join(
        c for c in unicodedata.normalize("NFD", str(text))
        if unicodedata.category(c) != "Mn"
    )


def normalize_col_name(col: str) -> str:
    """
    Normalise un nom de colonne :
      - supprime les accents
      - met en minuscules
      - remplace tout caractère non alphanumérique par un underscore
      - supprime les underscores de début/fin
    """
    col = str(col).strip()
    col = remove_accents(col)
    col = col.lower()
    col = re.sub(r"[^a-z0-9]+", "_", col)
    col = col.strip("_")
    return col


# ─────────────────────────────────────────────────────────────────────────────
# CHARGEMENT DES DONNÉES
# ─────────────────────────────────────────────────────────────────────────────

def load_data(
    path: str,
    sheet_name: str | int = 0,
    header: int = 0,
    encoding: str = "utf-8-sig",
    sep: str | None = None,
    from_zip: bool = False,
    zip_member: str | None = None,
) -> pd.DataFrame:
    """
    Charge un fichier CSV, Excel (.xlsx / .xls) ou un CSV dans un ZIP.

    Paramètres
    ----------
    path        : chemin absolu ou relatif au fichier
    sheet_name  : nom ou index de la feuille (Excel uniquement)
    header      : numéro de la ligne d'en-tête (0 = première ligne)
    encoding    : encodage du fichier CSV
    sep         : séparateur CSV (None = auto-détection)
    from_zip    : True si le fichier est un CSV compressé dans un ZIP
    zip_member  : nom du fichier CSV à l'intérieur du ZIP (None = premier)

    Retourne
    --------
    pd.DataFrame brut (aucun nettoyage effectué ici)
    """
    ext = os.path.splitext(path)[1].lower()
    abs_path = os.path.join(BASE_DIR, path) if not os.path.isabs(path) else path

    print(f"  [load] {os.path.basename(path)}", end="")

    if from_zip or ext == ".zip":
        with zipfile.ZipFile(abs_path) as z:
            member = zip_member or z.namelist()[0]
            with z.open(member) as f:
                df = pd.read_csv(f, sep=sep, engine="python", encoding=encoding, header=header)

    elif ext == ".csv":
        # Tentative UTF-8, puis latin-1 en fallback
        try:
            df = pd.read_csv(abs_path, sep=sep, engine="python", encoding=encoding, header=header)
        except UnicodeDecodeError:
            df = pd.read_csv(abs_path, sep=sep, engine="python", encoding="latin-1", header=header)

    elif ext == ".xlsx":
        df = pd.read_excel(abs_path, sheet_name=sheet_name, header=header, engine="openpyxl")

    elif ext == ".xls":
        df = pd.read_excel(abs_path, sheet_name=sheet_name, header=header, engine="xlrd")

    else:
        raise ValueError(f"Format non supporté : {ext}")

    print(f" → {df.shape[0]} lignes, {df.shape[1]} colonnes")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# NETTOYAGE DES COLONNES
# ─────────────────────────────────────────────────────────────────────────────

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise tous les noms de colonnes :
      - suppression des accents, espaces, caractères spéciaux
      - passage en minuscules avec séparation par underscore

    Exemple : "Code département" → "code_departement"
    """
    df = df.copy()
    df.columns = [normalize_col_name(c) for c in df.columns]
    # Dédoublonnage des noms de colonnes (suffixe _1, _2…)
    seen: dict = {}
    new_cols = []
    for col in df.columns:
        if col in seen:
            seen[col] += 1
            new_cols.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            new_cols.append(col)
    df.columns = new_cols
    return df


# ─────────────────────────────────────────────────────────────────────────────
# NETTOYAGE DES VALEURS
# ─────────────────────────────────────────────────────────────────────────────

def clean_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie les valeurs du DataFrame :
      1. Suppression des doublons
      2. Remplacement des indicateurs de valeur manquante (ND, s, n.a…)
      3. Conversion des colonnes numériques encodées en texte (virgules → points)
      4. Homogénéisation des types

    Les colonnes dont > 80 % des valeurs sont NaN sont signalées mais conservées.
    """
    df = df.copy()
    n_before = len(df)

    # Suppression des doublons stricts
    df = df.drop_duplicates()
    n_after = len(df)
    if n_before != n_after:
        print(f"    [clean] {n_before - n_after} doublon(s) supprimé(s)")

    # Marqueurs de valeur manquante fréquents dans les données INSEE
    NA_MARKERS = {"ND", "nd", "n.d.", "n.a.", "n.a", "NA", "N/A", "-", "s", "S", "ns", "NR", "nr"}

    for col in df.columns:
        if df[col].dtype == object:
            # Remplacer les marqueurs connus par NaN
            df[col] = df[col].replace(NA_MARKERS, np.nan)

            # Tenter une conversion numérique (gère la virgule décimale française)
            sample = df[col].dropna().head(200)
            # Forcer la conversion en str pour éviter les erreurs sur colonnes mixtes
            numeric_candidate = sample.astype(str).str.replace(",", ".", regex=False) \
                                                   .str.replace(r"\s", "", regex=True)
            n_numeric = pd.to_numeric(numeric_candidate, errors="coerce").notna().sum()
            if len(sample) > 0 and n_numeric / len(sample) > 0.80:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(",", ".", regex=False)
                    .str.replace(r"\s", "", regex=True)
                    .replace("nan", np.nan)
                )
                df[col] = pd.to_numeric(df[col], errors="coerce")

    # Signalement des colonnes très incomplètes (> 80 % NaN)
    na_rates = df.isna().mean()
    high_na = na_rates[na_rates > 0.80].index.tolist()
    if high_na:
        print(f"    [warn] Colonnes avec >80% NaN : {high_na}")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# FILTRAGE NOUVELLE-AQUITAINE
# ─────────────────────────────────────────────────────────────────────────────

def filter_region(df: pd.DataFrame, strategy: str = "auto") -> pd.DataFrame:
    """
    Filtre les lignes correspondant à la Nouvelle-Aquitaine.

    Stratégies disponibles (strategy) :
      "code_region"  — filtre sur une colonne contenant le code région (75)
      "code_dept"    — filtre sur une colonne code département (16…87)
      "nom_region"   — filtre textuel sur le nom de la région
      "codgeo"       — filtre sur CODGEO à partir du référentiel CODGEO_NA_REF
      "auto"         — détecte automatiquement la stratégie à appliquer

    Affiche le nombre de lignes avant/après filtrage.
    """
    df = df.copy()
    n_before = len(df)
    cols = set(df.columns)

    # ── Détection automatique ──────────────────────────────────────────────
    if strategy == "auto":
        if any(c in cols for c in ("code_region", "coderegion", "reg")):
            strategy = "code_region"
        elif "code_departement" in cols or "numero_de_departement" in cols:
            strategy = "code_dept"
        elif any(c in cols for c in ("nom_de_la_region", "nomregion", "libelle_de_region")):
            strategy = "nom_region"
        elif "codgeo" in cols and CODGEO_NA_REF:
            strategy = "codgeo"
        else:
            print("    [warn] Impossible de détecter une clé géographique pour le filtrage.")
            return df

    # ── code région ───────────────────────────────────────────────────────
    if strategy == "code_region":
        col = next((c for c in ("code_region", "coderegion", "reg") if c in cols), None)
        if col:
            df = df[pd.to_numeric(df[col], errors="coerce") == CODE_REGION_NA]

    # ── code département ──────────────────────────────────────────────────
    elif strategy == "code_dept":
        col = next(
            (c for c in ("code_departement", "numero_de_departement", "code_dept") if c in cols),
            None,
        )
        if col:
            # Robustesse : gère les float (ex. 16.0 → "16") et les string ("16")
            def _to_dept_str(x):
                try:
                    return str(int(float(str(x)))).zfill(2)
                except (ValueError, TypeError):
                    return str(x).strip().zfill(2)
            dept_str = df[col].map(_to_dept_str)
            df = df[dept_str.isin(DEPTS_NA)]

    # ── nom de région (textuel, robuste aux variations) ───────────────────
    elif strategy == "nom_region":
        col = next(
            (c for c in ("nom_de_la_region", "nomregion", "libelle_de_region") if c in cols),
            None,
        )
        if col:
            norm = df[col].astype(str).apply(remove_accents).str.lower().str.strip()
            norm = norm.str.replace(r"[-_\s]+", " ", regex=True).str.strip()
            df = df[norm.isin(["nouvelle aquitaine"])]

    # ── CODGEO (référentiel pré-calculé) ──────────────────────────────────
    elif strategy == "codgeo":
        col = next((c for c in ("codgeo", "code_commune") if c in cols), None)
        if col and CODGEO_NA_REF:
            df = df[df[col].astype(str).isin(CODGEO_NA_REF)]

    n_after = len(df)
    print(f"    [filter] {n_before} → {n_after} lignes (Nouvelle-Aquitaine, stratégie '{strategy}')")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# NORMALISATION DES CLÉS GÉOGRAPHIQUES
# ─────────────────────────────────────────────────────────────────────────────

def normalize_geo_keys(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardise les clés géographiques dans les colonnes suivantes :
      - code_commune    : 5 caractères (2 dept + 3 commune), ex. "33063"
      - code_departement: 2 caractères avec zéro initial, ex. "09"
      - code_region     : entier, ex. 75

    Les différentes variantes de nommage sont détectées et renommées.
    """
    df = df.copy()
    cols = set(df.columns)

    # ── code_commune (CODGEO) ────────────────────────────────────────────
    commune_candidates = ("codgeo", "codecommune", "code_commune")
    for cand in commune_candidates:
        if cand in cols and "code_commune" not in cols:
            df = df.rename(columns={cand: "code_commune"})
            break
    if "code_commune" in df.columns:
        # Vérifie si la colonne contient déjà un CODGEO 5 chiffres ou juste la partie locale
        raw_len = df["code_commune"].dropna().astype(str).str.strip().str.len().max()
        if raw_len is not np.nan and raw_len <= 3 and "code_departement" in df.columns:
            # Reconstruction : code_departement (2 chiffres) + code_commune (3 chiffres)
            dept_str = df["code_departement"].astype(str).str.strip().str.zfill(2)
            comm_str = df["code_commune"].astype(str).str.strip().str.zfill(3)
            df["code_commune"] = dept_str + comm_str
        else:
            df["code_commune"] = df["code_commune"].astype(str).str.strip().str.zfill(5)

    # ── code_departement ─────────────────────────────────────────────────
    dept_candidates = (
        "codedepartement", "code_dept", "numero_de_departement",
        "dep", "code_departement_1",  # doublon éventuel
    )
    for cand in dept_candidates:
        if cand in cols and "code_departement" not in cols:
            df = df.rename(columns={cand: "code_departement"})
            break
    if "code_departement" in df.columns:
        def _fmt_dept(x):
            try:
                return str(int(float(str(x)))).zfill(2)
            except (ValueError, TypeError):
                return str(x).strip().zfill(2)
        df["code_departement"] = df["code_departement"].map(_fmt_dept)

    # ── code_region ───────────────────────────────────────────────────────
    region_candidates = ("coderegion", "code_region_1", "reg")
    for cand in region_candidates:
        if cand in cols and "code_region" not in cols:
            df = df.rename(columns={cand: "code_region"})
            break
    if "code_region" in df.columns:
        df["code_region"] = pd.to_numeric(df["code_region"], errors="coerce").astype("Int64")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# CONTRÔLES QUALITÉ
# ─────────────────────────────────────────────────────────────────────────────

def quality_check(df: pd.DataFrame, label: str) -> None:
    """
    Affiche un rapport de qualité pour un dataset :
      - dimensions
      - valeurs uniques des clés géographiques
      - taux de valeurs manquantes (colonnes > 10 %)
      - vérification que seule la NA est présente
    """
    print(f"\n{'─'*60}")
    print(f"  QUALITÉ : {label}")
    print(f"{'─'*60}")
    print(f"  Shape            : {df.shape}")

    geo_cols = [c for c in ("code_region", "code_departement", "code_commune") if c in df.columns]
    for col in geo_cols:
        uniq = sorted(df[col].dropna().unique())
        print(f"  {col:22} : {len(uniq)} valeurs uniques — {uniq[:10]}{'…' if len(uniq)>10 else ''}")

    # Vérification de l'exclusivité NA
    if "code_departement" in df.columns:
        depts_out = set(df["code_departement"].dropna().astype(str)) - DEPTS_NA
        if depts_out:
            print(f"  [ALERTE] Départements hors NA détectés : {depts_out}")
        else:
            print("  [OK] Tous les départements appartiennent à la Nouvelle-Aquitaine")

    # Taux de NaN > 10 %
    na_rates = df.isna().mean()
    noisy = na_rates[na_rates > 0.10].sort_values(ascending=False)
    if not noisy.empty:
        print(f"  Colonnes avec >10% NaN :")
        for col, rate in noisy.items():
            print(f"    {col:40} {rate:.1%}")

    # Clés de jointure disponibles
    join_keys = [c for c in ("code_commune", "code_departement", "annee", "year") if c in df.columns]
    print(f"  Clés de jointure : {join_keys}")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE PAR DATASET
# ─────────────────────────────────────────────────────────────────────────────

def prepare_dataset(
    path: str,
    label: str,
    filter_strategy: str = "auto",
    sheet_name: str | int = 0,
    header: int = 0,
    from_zip: bool = False,
    zip_member: str | None = None,
    year: int | None = None,
    extra_drops: list[str] | None = None,
) -> pd.DataFrame:
    """
    Pipeline complet pour un dataset :
      load → clean_columns → clean_values → filter_region
           → normalize_geo_keys → ajout colonne annee → export CSV

    Paramètres
    ----------
    path            : chemin du fichier source
    label           : nom court du dataset (pour les logs et l'export)
    filter_strategy : stratégie de filtrage NA ("auto", "code_region", etc.)
    sheet_name      : feuille Excel à lire
    header          : ligne d'en-tête dans le fichier
    from_zip        : le fichier est dans un ZIP
    zip_member      : nom du fichier dans le ZIP
    year            : année à ajouter en colonne (ex. 2016, 2020)
    extra_drops     : liste de colonnes à supprimer après chargement

    Retourne
    --------
    pd.DataFrame nettoyé, filtré NA, avec clés géographiques normalisées
    """
    print(f"\n{'='*60}")
    print(f"  PIPELINE : {label}")
    print(f"{'='*60}")

    # 1. Chargement
    df = load_data(
        path,
        sheet_name=sheet_name,
        header=header,
        from_zip=from_zip,
        zip_member=zip_member,
    )

    # 2. Suppression de colonnes parasites optionnelle
    if extra_drops:
        df = df.drop(columns=[c for c in extra_drops if c in df.columns], errors="ignore")

    # 3. Normalisation des noms de colonnes
    df = clean_columns(df)

    # 4. Nettoyage des valeurs
    df = clean_values(df)

    # 5. Filtrage Nouvelle-Aquitaine
    df = filter_region(df, strategy=filter_strategy)

    # 6. Standardisation des clés géographiques
    df = normalize_geo_keys(df)

    # 7. Ajout de la colonne année pour faciliter les jointures temporelles
    if year is not None and "annee" not in df.columns:
        df["annee"] = year

    # 8. Suppression des lignes sans clé géographique principale
    if "code_commune" in df.columns:
        n_before = len(df)
        df = df[df["code_commune"].notna() & (df["code_commune"] != "00000")]
        n_dropped = n_before - len(df)
        if n_dropped:
            print(f"    [clean] {n_dropped} ligne(s) sans code_commune supprimée(s)")

    # 9. Contrôle qualité
    quality_check(df, label)

    # 10. Export CSV
    out_path = os.path.join(OUTPUT_DIR, f"{label.lower().replace(' ', '_')}.csv")
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"  [export] → {out_path}")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# CONSTRUCTION DU RÉFÉRENTIEL CODGEO NOUVELLE-AQUITAINE
# ─────────────────────────────────────────────────────────────────────────────

def build_codgeo_reference() -> pd.DataFrame:
    """
    Construit le référentiel géographique de la Nouvelle-Aquitaine
    à partir du fichier Population 2020 (source la plus complète).

    Remplit CODGEO_NA_REF avec l'ensemble des codes communes INSEE.
    Retourne le DataFrame de référence.
    """
    global CODGEO_NA_REF

    print("\n" + "=" * 60)
    print("  CONSTRUCTION DU RÉFÉRENTIEL CODGEO NOUVELLE-AQUITAINE")
    print("=" * 60)

    df = load_data(
        "Dataset 2020/Population.xlsx",
        sheet_name="Communes",
        header=7,
    )
    df = clean_columns(df)

    # Filtrage par code région 75
    df = df[pd.to_numeric(df["code_region"], errors="coerce") == CODE_REGION_NA].copy()

    # Construction du CODGEO 5 caractères
    df["code_departement"] = df["code_departement"].astype(str).str.zfill(2)
    df["code_commune_local"] = df["code_commune"].astype(str).str.zfill(3)
    df["code_commune"] = df["code_departement"] + df["code_commune_local"]
    df = df.drop(columns=["code_commune_local"])

    # Remplissage du référentiel global
    CODGEO_NA_REF = set(df["code_commune"].unique())
    print(f"    [ref] {len(CODGEO_NA_REF)} communes Nouvelle-Aquitaine indexées")

    df = df.rename(columns={"nom_de_la_commune": "nom_commune"})
    df["annee"] = 2020
    df["code_region"] = CODE_REGION_NA

    quality_check(df, "Référentiel Communes NA")
    out_path = os.path.join(OUTPUT_DIR, "ref_communes_na.csv")
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"  [export] → {out_path}")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# GESTION DES DONNÉES ÉLECTORALES (extensible)
# ─────────────────────────────────────────────────────────────────────────────

def prepare_electoral_dataset(
    path: str,
    label: str,
    year: int,
    code_commune_col: str = "code_commune",
    header: int = 0,
    sheet_name: str | int = 0,
) -> pd.DataFrame:
    """
    Pipeline spécialisé pour les données électorales.
    Structure attendue minimale :
      - code commune INSEE (à préciser via code_commune_col)
      - inscrits, votants, abstentions, résultats par parti

    S'appuie sur le référentiel CODGEO_NA_REF pour le filtrage.
    Ce pipeline est préconfiguré pour être facilement étendu.
    """
    print(f"\n{'='*60}")
    print(f"  PIPELINE ELECTORAL : {label} ({year})")
    print(f"{'='*60}")

    df = load_data(path, sheet_name=sheet_name, header=header)
    df = clean_columns(df)
    df = clean_values(df)

    # Renommage flexible de la colonne commune
    if code_commune_col in df.columns and code_commune_col != "code_commune":
        df = df.rename(columns={code_commune_col: "code_commune"})

    df = filter_region(df, strategy="codgeo")
    df = normalize_geo_keys(df)
    df["annee"] = year

    quality_check(df, label)

    out_path = os.path.join(OUTPUT_DIR, f"electoral_{label.lower().replace(' ', '_')}_{year}.csv")
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"  [export] → {out_path}")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# FUSION DES DATASETS (MERGE)
# ─────────────────────────────────────────────────────────────────────────────

def merge_indicators(datasets: dict[str, pd.DataFrame], on: list[str] = None) -> pd.DataFrame:
    """
    Fusionne plusieurs datasets d'indicateurs sur les clés géographiques communes.

    Stratégie de merge :
      - jointure LEFT sur code_commune + annee (quand disponible)
      - suffixes pour distinguer les colonnes homonymes
      - rapport sur le taux de correspondance entre chaque dataset

    Paramètres
    ----------
    datasets : dict { "label" : DataFrame }
    on       : liste des colonnes clés (défaut : ["code_commune"])

    Retourne
    --------
    DataFrame fusionné
    """
    if on is None:
        on = ["code_commune"]

    print(f"\n{'='*60}")
    print(f"  FUSION DES DATASETS — clés : {on}")
    print(f"{'='*60}")

    base_label, base_df = next(iter(datasets.items()))
    result = base_df.copy()
    print(f"  Base : {base_label} ({len(result)} lignes)")

    for label, df in list(datasets.items())[1:]:
        # Clés réellement présentes dans les deux DataFrames
        join_cols = [c for c in on if c in result.columns and c in df.columns]
        if not join_cols:
            print(f"  [skip] {label} — aucune clé commune avec la base")
            continue

        # Suffixe pour éviter les conflits de noms de colonnes
        suffix = f"_{label[:6].lower().replace(' ', '_')}"
        n_before = len(result)
        result = result.merge(df, on=join_cols, how="left", suffixes=("", suffix))
        matched = result[join_cols[0]].notna().sum()
        print(f"  + {label:30} → {len(result)} lignes | taux jointure : {matched/n_before:.1%}")

    print(f"\n  Dataset fusionné : {result.shape}")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline() -> dict[str, pd.DataFrame]:
    """
    Exécute le pipeline ETL complet :
      1. Référentiel communes NA
      2. Indicateurs 2020
      3. Indicateurs 2016
      4. Fusion finale
      5. Export du dataset consolidé

    Retourne un dictionnaire {label: DataFrame} de tous les datasets produits.
    """
    print("\n" + "█" * 60)
    print("  ELECTIO-ANALYTICS — PIPELINE ETL NOUVELLE-AQUITAINE")
    print("█" * 60)

    results: dict[str, pd.DataFrame] = {}

    # ── 0. Référentiel ────────────────────────────────────────────────────
    results["ref_communes"] = build_codgeo_reference()

    # ── 1. EMPLOI 2020 ────────────────────────────────────────────────────
    # Données emploi + économie au niveau commune, issue du dispositif Flores/Base CC
    results["emploi_2020"] = prepare_dataset(
        path="Dataset 2020/Emploi.csv",
        label="emploi_2020",
        filter_strategy="code_region",   # colonne "codeRegion" = 75
        year=2020,
    )

    # ── 2. REVENUS 2020 ───────────────────────────────────────────────────
    # FiLoSoFi 2020 — médiane, taux de pauvreté au niveau commune
    results["revenus_2020"] = prepare_dataset(
        path="Dataset 2020/Revenus.xlsx",
        label="revenus_2020",
        filter_strategy="codgeo",        # pas de colonne région → via référentiel
        sheet_name="COM",
        header=5,
        year=2020,
    )

    # ── 3. POPULATION 2020 ────────────────────────────────────────────────
    # Populations légales communales (recensement)
    results["population_2020"] = prepare_dataset(
        path="Dataset 2020/Population.xlsx",
        label="population_2020",
        filter_strategy="code_region",
        sheet_name="Communes",
        header=7,
        year=2020,
    )

    # ── 4. DÉLINQUANCE 2020 ───────────────────────────────────────────────
    # Données département, nécessite une agrégation si jointure commune
    results["delinquance_2020"] = prepare_dataset(
        path="Dataset 2020/Délinquance.xlsx",
        label="delinquance_2020",
        filter_strategy="code_dept",     # colonne "Numéro de département"
        sheet_name="par départements",
        header=0,
        year=2020,
    )

    # ── 5. FORMATION 2020 ─────────────────────────────────────────────────
    # Niveaux de diplôme par commune (extrait du RP 2020 — fichier détail)
    results["formation_2020"] = prepare_dataset(
        path="Dataset 2020/TD_FOR2_2020_csv.zip",
        label="formation_2020",
        filter_strategy="codgeo",
        from_zip=True,
        year=2020,
    )

    # ── 6. POPULATION & EMPLOI 2016 ───────────────────────────────────────
    # Indicateurs démographiques et emploi 2016 au niveau commune
    results["pop_emploi_2016"] = prepare_dataset(
        path="Dataset 2016/Population & emploi.csv",
        label="pop_emploi_2016",
        filter_strategy="codgeo",        # pas de colonne région → via référentiel
        year=2016,
    )

    # ── 7. REVENUS 2016 ───────────────────────────────────────────────────
    # FiLoSoFi 2016 — distribution des revenus disponibles
    results["revenus_2016"] = prepare_dataset(
        path="Dataset 2016/Revenus.xls",
        label="revenus_2016",
        filter_strategy="codgeo",
        sheet_name="ENSEMBLE",
        header=5,
        year=2016,
    )

    # ── 8. DIPLÔME 2016 ───────────────────────────────────────────────────
    # Formation / niveau de diplôme par commune (recensement 2016)
    results["diplome_2016"] = prepare_dataset(
        path="Dataset 2016/Diplôme.xls",
        label="diplome_2016",
        filter_strategy="code_region",   # colonne "REG" = 75
        sheet_name="COM_2016",
        header=5,
        year=2016,
    )

    # ── 9. DÉLINQUANCE 2016 ───────────────────────────────────────────────
    # Données département / indicateur / année (format long)
    results["delinquance_2016"] = prepare_dataset(
        path="Dataset 2016/Délinquance.csv",
        label="delinquance_2016",
        filter_strategy="code_region",   # colonne "Code_region" = 75
        year=None,                        # la colonne "annee" est déjà présente
    )

    # ── 10. FUSION DES INDICATEURS COMMUNE (2020) ─────────────────────────
    # On fusionne les datasets à granularité commune pour 2020
    commune_2020 = {
        "emploi_2020":     results["emploi_2020"],
        "revenus_2020":    results["revenus_2020"],
        "population_2020": results["population_2020"],
    }
    merged_2020 = merge_indicators(commune_2020, on=["code_commune"])
    out_2020 = os.path.join(OUTPUT_DIR, "indicateurs_communes_na_2020.csv")
    merged_2020.to_csv(out_2020, index=False, encoding="utf-8")
    print(f"\n  [export FUSIONNÉ 2020] → {out_2020}")
    results["merged_2020"] = merged_2020

    # ── 11. FUSION DES INDICATEURS COMMUNE (2016) ─────────────────────────
    commune_2016 = {
        "pop_emploi_2016": results["pop_emploi_2016"],
        "revenus_2016":    results["revenus_2016"],
        "diplome_2016":    results["diplome_2016"],
    }
    merged_2016 = merge_indicators(commune_2016, on=["code_commune"])
    out_2016 = os.path.join(OUTPUT_DIR, "indicateurs_communes_na_2016.csv")
    merged_2016.to_csv(out_2016, index=False, encoding="utf-8")
    print(f"\n  [export FUSIONNÉ 2016] → {out_2016}")
    results["merged_2016"] = merged_2016

    # ── 12. RAPPORT FINAL ─────────────────────────────────────────────────
    print("\n" + "█" * 60)
    print("  RÉSUMÉ DU PIPELINE")
    print("█" * 60)
    for label, df in results.items():
        print(f"  {label:35} : {df.shape[0]:6} lignes × {df.shape[1]:3} colonnes")

    print(f"\n  Fichiers exportés dans : {OUTPUT_DIR}")
    print("█" * 60 + "\n")

    return results


# ─────────────────────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    all_datasets = run_pipeline()
