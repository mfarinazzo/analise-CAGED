"""
Apply outlier patch for August and September 2023 directly to projeto_caged.db.

Strategy (sequential):
- Step 1: Fix August 2023 by replacing per-group sums (salary/age) only for outliers,
  using pooled mean baseline from May, June, July 2023.
- Step 2: Build a corrected August month (in-memory) and use it in the baseline for
  September (baseline = June, July, and August FIXED). Then fix September with the same
  filtered replacement rule.

Safety:
- Creates a timestamped backup of the DB before modifying.
- Performs changes in a single transaction: delete rows for the two months
  and insert corrected rows.

Threshold (filtered): flag a group as outlier if any condition holds:
- mean_sal > 5000 OR
- mean_sal > 1.5 * prior_max_mean_sal (across baseline months) OR
- mean_sal > 1.5 * mean_sal_base (pooled baseline mean)
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
import shutil
import sqlite3
from typing import List, Dict

import pandas as pd


BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / 'projeto_caged.db'

AGG_KEYS = [
    'municipio',
    'cnae20subclasse',
    'grau_instrucao',
    'genero',
    'raca_cor',
]


def read_months(conn: sqlite3.Connection, months: List[str]) -> pd.DataFrame:
    qs = ','.join('?' for _ in months)
    sql = (
        'SELECT data, municipio, cnae20subclasse, grau_instrucao, genero, raca_cor, '
        '       soma_salario, soma_idade, total_admissoes '
        f'FROM dados_agregados WHERE data IN ({qs})'
    )
    return pd.read_sql(sql, conn, params=months)


def per_group_means(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    denom = out['total_admissoes'].replace({0: pd.NA})
    out['mean_sal'] = out['soma_salario'] / denom
    out['mean_idd'] = out['soma_idade'] / denom
    return out


def pooled_means(df: pd.DataFrame) -> pd.DataFrame:
    grp = (
        df.groupby(AGG_KEYS, dropna=False)
        .agg(
            soma_salario=('soma_salario', 'sum'),
            soma_idade=('soma_idade', 'sum'),
            total_admissoes=('total_admissoes', 'sum'),
        )
        .reset_index()
    )
    denom = grp['total_admissoes'].replace({0: pd.NA})
    grp['mean_sal'] = grp['soma_salario'] / denom
    grp['mean_idd'] = grp['soma_idade'] / denom
    return grp


def compute_corrected_month(df_target: pd.DataFrame, df_baseline: pd.DataFrame, label: str) -> pd.DataFrame:
    """
    Returns a corrected copy of df_target with sums replaced for outlier groups
    based on pooled baseline means.
    """
    cur = per_group_means(df_target)
    base = pooled_means(df_baseline)[AGG_KEYS + ['mean_sal', 'mean_idd']]
    merged = cur.merge(base, on=AGG_KEYS, how='left', suffixes=('', '_base'))

    # prior max mean across baseline months
    base_means = per_group_means(df_baseline)
    prior_max = (
        base_means.groupby(AGG_KEYS, dropna=False)['mean_sal']
        .max()
        .reset_index()
        .rename(columns={'mean_sal': 'prior_max_mean_sal'})
    )
    merged = merged.merge(prior_max, on=AGG_KEYS, how='left')

    # Outlier condition
    cond = (
        (merged['mean_sal'] > 5000)
        | (merged['mean_sal'] > 1.5 * merged['prior_max_mean_sal'])
        | (merged['mean_sal'] > 1.5 * merged['mean_sal_base'])
    )
    merged['apply_fix'] = cond.fillna(False)

    # New sums
    merged['new_soma_salario'] = merged['soma_salario']
    merged['new_soma_idade'] = merged['soma_idade']
    sel = merged['apply_fix']
    merged.loc[sel, 'new_soma_salario'] = merged.loc[sel, 'mean_sal_base'] * merged.loc[sel, 'total_admissoes']
    merged.loc[sel, 'new_soma_idade'] = merged.loc[sel, 'mean_idd_base'] * merged.loc[sel, 'total_admissoes']

    # Build final df for this month
    final = merged[AGG_KEYS + ['new_soma_salario', 'new_soma_idade', 'total_admissoes']].copy()
    final = final.rename(columns={'new_soma_salario': 'soma_salario', 'new_soma_idade': 'soma_idade'})
    final.insert(0, 'data', label)
    return final


def overall_mean(df: pd.DataFrame) -> float:
    s = df['soma_salario'].sum()
    n = df['total_admissoes'].sum()
    return float(s / n) if n else float('nan')


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"Banco não encontrado: {DB_PATH}")

    # Backup
    backup = DB_PATH.with_name(f"{DB_PATH.stem}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")
    shutil.copy2(DB_PATH, backup)
    print(f"Backup criado: {backup}")

    with sqlite3.connect(DB_PATH) as conn:
        print('Conectado ao banco:', DB_PATH)

        # Step 1: Agosto (baseline: 2023-05, 2023-06, 2023-07)
        months_aug = ['2023-08', '2023-05', '2023-06', '2023-07']
        df_aug_full = read_months(conn, months_aug)
        df_aug_target = df_aug_full[df_aug_full['data'] == '2023-08'].copy()
        df_aug_base = df_aug_full[df_aug_full['data'].isin(['2023-05', '2023-06', '2023-07'])].copy()
        df_aug_fixed = compute_corrected_month(df_aug_target, df_aug_base, '2023-08')
        print(f"Agosto: média atual={overall_mean(df_aug_target):,.2f} | proposta={overall_mean(df_aug_fixed):,.2f}")

        # Step 2: Setembro (baseline sequencial: 2023-06, 2023-07 + Agosto corrigido)
        months_sep = ['2023-09', '2023-06', '2023-07']
        df_sep_from_db = read_months(conn, months_sep)
        df_sep_target = df_sep_from_db[df_sep_from_db['data'] == '2023-09'].copy()
        df_sep_base = df_sep_from_db[df_sep_from_db['data'].isin(['2023-06', '2023-07'])].copy()
        base_cols = ['data'] + AGG_KEYS + ['soma_salario', 'soma_idade', 'total_admissoes']
        df_sep_base = pd.concat([df_sep_base[base_cols], df_aug_fixed[base_cols]], ignore_index=True)
        df_sep_fixed = compute_corrected_month(df_sep_target, df_sep_base, '2023-09')
        print(f"Setembro: média atual={overall_mean(df_sep_target):,.2f} | proposta={overall_mean(df_sep_fixed):,.2f}")

        # Apply changes in a transaction: delete + insert
        # Usa controle manual de autocommit: sqlite3 em Python inicia transação implícita ao primeiro write.
        try:
            conn.execute("DELETE FROM dados_agregados WHERE data IN ('2023-08','2023-09')")
            df_aug_fixed.to_sql('dados_agregados', conn, if_exists='append', index=False, chunksize=10000)
            df_sep_fixed.to_sql('dados_agregados', conn, if_exists='append', index=False, chunksize=10000)
            conn.commit()
            print('Atualização aplicada com sucesso para 2023-08 e 2023-09.')
        except Exception:
            conn.rollback()
            raise

        # Verify new monthly averages
        cur = conn.cursor()
        for row in cur.execute(
            "SELECT data, SUM(soma_salario)*1.0/SUM(total_admissoes) AS media "
            "FROM dados_agregados WHERE data IN ('2023-08','2023-09') GROUP BY data ORDER BY data"
        ):
            print(f"Verificação pós-aplicação -> {row[0]}: média={row[1]:,.2f}")


if __name__ == '__main__':
    main()
