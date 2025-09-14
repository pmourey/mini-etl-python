#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import sqlite3
from datetime import datetime
import os

def extract(csv_path):
    print("📥 Extraction des données...")
    return pd.read_csv(csv_path)

def transform(df):
    print("🔧 Transformation des données...")
    df.dropna(inplace=True)
    df['Nom'] = df['Nom'].str.title()
    df['Anciennete'] = df['Date_Entree'].apply(lambda x: datetime.now().year - int(x.split("-")[0]))
    return df

def load(df, db_path):
    print("📤 Chargement dans la base SQLite...")
    conn = sqlite3.connect(db_path)
    df.to_sql("employes", conn, if_exists="replace", index=False)
    print("✅ Données insérées avec succès.")
    conn.close()

def main():
    csv_file = "employes.csv"
    db_file = "employes.db"

    if not os.path.exists(csv_file):
        print(f"❌ Fichier {csv_file} introuvable.")
        return

    df = extract(csv_file)
    df = transform(df)
    load(df, db_file)

    print("🎉 Pipeline ETL terminé.")

if __name__ == "__main__":
    main()
