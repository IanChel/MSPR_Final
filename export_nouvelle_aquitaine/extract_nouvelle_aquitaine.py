from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

# Codes departements de la region Nouvelle-Aquitaine
NOUVELLE_AQUITAINE_DEPARTMENTS = {
    "16",  # Charente
    "17",  # Charente-Maritime
    "19",  # Corrèze
    "23",  # Creuse
    "24",  # Dordogne
    "33",  # Gironde
    "40",  # Landes
    "47",  # Lot-et-Garonne
    "64",  # Pyrénées-Atlantiques
    "79",  # Deux-Sèvres
    "86",  # Vienne
    "87",  # Haute-Vienne
}

TARGET_YEARS = [2012, 2017]
TOUR_SHEETS = {
    1: "data_election_tour1",
    2: "data_election_tour2",
}


def normalize_dept_code(value: str) -> str:
    return value.strip().zfill(2)


def extract_nouvelle_aquitaine_for_tour(data_dir: Path, output_csv: Path, tour: int) -> int:
    if tour not in TOUR_SHEETS:
        raise ValueError(f"Tour non supporte: {tour}. Valeurs autorisees: {sorted(TOUR_SHEETS)}")

    sheet_name = TOUR_SHEETS[tour]
    frames: list[pd.DataFrame] = []
    reference_columns_2012: list[str] | None = None

    for year in TARGET_YEARS:
        source_file = data_dir / f"data_election_{year}.xlsx"
        if not source_file.exists():
            raise FileNotFoundError(f"Fichier source introuvable: {source_file}")

        df = pd.read_excel(source_file, sheet_name=sheet_name)

        # On retient la structure 2012 comme reference de sortie.
        if year == 2012:
            reference_columns_2012 = [c for c in df.columns if "Panneau" not in str(c)]

        if "code_departement" not in df.columns:
            raise ValueError(f"Colonne 'code_departement' absente dans {source_file}")

        # Normaliser les codes de département
        normalized_codes = df["code_departement"].astype(str).map(normalize_dept_code)
        
        # Créer le masque de filtrage
        mask = normalized_codes.isin(NOUVELLE_AQUITAINE_DEPARTMENTS)
        
        # Appliquer le filtre et copier
        filtered = df[mask].copy()
        
        # Remplacer les codes de département par les codes normalisés
        filtered.loc[:, "code_departement"] = normalized_codes[mask].values

        # Nettoyage : on supprime les colonnes 'N°Panneau' qui existent en 2017 mais pas en 2012
        panneau_cols = [c for c in filtered.columns if "Panneau" in str(c)]
        filtered = filtered.drop(columns=panneau_cols)

        # Harmonisation 2017 -> schema 2012
        if year == 2017:
            if "Blancs et nuls" not in filtered.columns and {"Blancs", "Nuls"}.issubset(filtered.columns):
                blancs = pd.to_numeric(filtered["Blancs"], errors="coerce").fillna(0)
                nuls = pd.to_numeric(filtered["Nuls"], errors="coerce").fillna(0)
                filtered["Blancs et nuls"] = blancs + nuls

            if "% BlNuls/Ins" not in filtered.columns and {"Blancs et nuls", "Inscrits"}.issubset(filtered.columns):
                blnuls = pd.to_numeric(filtered["Blancs et nuls"], errors="coerce")
                inscrits = pd.to_numeric(filtered["Inscrits"], errors="coerce")
                filtered["% BlNuls/Ins"] = (blnuls * 100 / inscrits.where(inscrits != 0)).round(2)

            if "% BlNuls/Vot" not in filtered.columns and {"Blancs et nuls", "Votants"}.issubset(filtered.columns):
                blnuls = pd.to_numeric(filtered["Blancs et nuls"], errors="coerce")
                votants = pd.to_numeric(filtered["Votants"], errors="coerce")
                filtered["% BlNuls/Vot"] = (blnuls * 100 / votants.where(votants != 0)).round(2)

            # Aligner strictement sur les colonnes 2012 pour eviter les colonnes en trop (.10, Blancs, Nuls, ...)
            if reference_columns_2012 is not None:
                for col in reference_columns_2012:
                    if col not in filtered.columns:
                        filtered[col] = ""
                filtered = filtered[reference_columns_2012]

        filtered.insert(0, "Région", "Nouvelle-Aquitaine")
        filtered.insert(1, "Année", year)
        filtered.insert(2, "Tour", tour)

        frames.append(filtered)

    # Concaténation des années
    merged = pd.concat(frames, ignore_index=True)

    # Remplacer les valeurs manquantes par vide pour eviter l'affichage NaN
    merged = merged.fillna("")

    merged.to_csv(output_csv, index=False, encoding="utf-8-sig")
    return len(merged)


def main() -> None:
    workspace_root = Path(__file__).resolve().parent.parent
    default_data_dir = workspace_root / "Data election"
    default_output = workspace_root / "export_nouvelle_aquitaine" / "nouvelle_aquitaine_2012_2017_tour1.csv"
    default_output_tour2 = workspace_root / "export_nouvelle_aquitaine" / "nouvelle_aquitaine_2012_2017_tour2.csv"

    parser = argparse.ArgumentParser(
        description="Extrait les donnees elections 2012 et 2017 (tours 1 et 2) pour la Nouvelle-Aquitaine."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=default_data_dir,
        help="Dossier contenant data_election_2012.xlsx et data_election_2017.xlsx",
    )
    parser.add_argument(
        "--output-tour1",
        type=Path,
        default=default_output,
        help="Chemin du CSV de sortie pour le 1er tour",
    )
    parser.add_argument(
        "--output-tour2",
        type=Path,
        default=default_output_tour2,
        help="Chemin du CSV de sortie pour le 2e tour",
    )

    args = parser.parse_args()

    if not args.data_dir.exists():
        raise FileNotFoundError(f"Dossier source introuvable: {args.data_dir}")

    args.output_tour1.parent.mkdir(parents=True, exist_ok=True)
    args.output_tour2.parent.mkdir(parents=True, exist_ok=True)

    row_count_tour1 = extract_nouvelle_aquitaine_for_tour(args.data_dir, args.output_tour1, 1)
    row_count_tour2 = extract_nouvelle_aquitaine_for_tour(args.data_dir, args.output_tour2, 2)
    print(f"Extraction tour 1 terminee: {row_count_tour1} lignes ecrites dans {args.output_tour1}")
    print(f"Extraction tour 2 terminee: {row_count_tour2} lignes ecrites dans {args.output_tour2}")


if __name__ == "__main__":
    main()