#!/usr/bin/env python3
"""
Script: Duplikasi dataset Olist untuk memenuhi karakteristik Volume Big Data
Hasil: ~500MB+ data (dari ~122MB asli)
"""
import pandas as pd
import os
import uuid

DATA_DIR = "data/raw"
MULTIPLY = 5  # duplikasi 5x

files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]

for fname in files:
    path = os.path.join(DATA_DIR, fname)
    print(f"Menduplikasi {fname}...")
    df = pd.read_csv(path)
    original_len = len(df)

    frames = [df]
    for i in range(MULTIPLY - 1):
        df_copy = df.copy()
        # Ubah ID agar tidak duplikat
        for col in df_copy.columns:
            if 'id' in col.lower() and df_copy[col].dtype == object:
                df_copy[col] = df_copy[col].apply(lambda x: str(uuid.uuid4())[:32] if pd.notna(x) else x)
        frames.append(df_copy)

    df_big = pd.concat(frames, ignore_index=True)
    out_path = os.path.join(DATA_DIR, f"big_{fname}")
    df_big.to_csv(out_path, index=False)
    size_mb = os.path.getsize(out_path) / 1024 / 1024
    print(f"  ✓ {original_len} baris → {len(df_big)} baris ({size_mb:.1f} MB)")

print("\nDuplikasi selesai!")
