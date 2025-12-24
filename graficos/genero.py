from __future__ import annotations

import pandas as pd
from pathlib import Path
from typing import Dict

from .common import connect, save_json


GENERO_MAP = {'1': 'Homens', '3': 'Mulheres'}


def gerar_json() -> Path:
    sql = """
        SELECT data, genero,
               SUM(soma_salario)*1.0/SUM(total_admissoes) AS salario_medio
        FROM dados_agregados
        WHERE total_admissoes > 0
        GROUP BY data, genero
        ORDER BY data
    """
    with connect() as conn:
        df = pd.read_sql(sql, conn)

    # Garantias de tipo/ordem
    df['data'] = pd.to_datetime(df['data'])
    df['genero'] = df['genero'].astype(str)
    df = df.sort_values(['data', 'genero'])

    # Pivota para Highcharts: uma série por gênero
    pivot = df.pivot(index='data', columns='genero', values='salario_medio')
    categorias = [d.strftime('%Y-%m') for d in pivot.index]

    series = []
    for cod, nome in GENERO_MAP.items():
        if cod in pivot.columns:
            col = pivot[cod].round(2)
            vals = col.where(col.notna(), None).tolist()
            series.append({'name': nome, 'data': vals})

    payload = {
        'title': 'Salário Médio de Admissão por Gênero',
        'subtitle': 'Fonte: projeto_caged.db (dados_agregados)',
        'yAxisTitle': 'R$ (média)',
        'categories': categorias,
        'series': series,
    }
    return save_json('genero', payload)


def imprimir_disparidades() -> None:
    with connect() as conn:
        # 2025
        df_2025 = pd.read_sql(
            """
            SELECT genero, SUM(soma_salario)*1.0/SUM(total_admissoes) AS media
            FROM dados_agregados
            WHERE data LIKE '2025-%' AND total_admissoes > 0
            GROUP BY genero
            """,
            conn,
        )
        df_all = pd.read_sql(
            """
            SELECT genero, SUM(soma_salario)*1.0/SUM(total_admissoes) AS media
            FROM dados_agregados
            WHERE total_admissoes > 0
            GROUP BY genero
            """,
            conn,
        )

    def gap(df: pd.DataFrame) -> Dict[str, float]:
        m = float(df.loc[df['genero'].astype(str) == '1', 'media'].values[0]) if '1' in df['genero'].astype(str).values else float('nan')
        f = float(df.loc[df['genero'].astype(str) == '3', 'media'].values[0]) if '3' in df['genero'].astype(str).values else float('nan')
        hiato = (f / m) * 100 if m and m == m and f == f else float('nan')
        diferenca = (1 - f / m) * 100 if m and m == m and f == f else float('nan')
        return {'masc': m, 'fem': f, 'hiato_pct': hiato, 'gap_pct': diferenca}

    g2025 = gap(df_2025)
    gall = gap(df_all)

    print('=== Disparidade por Gênero ===')
    print('2025:')
    print(f"  Salário médio Masc: R$ {g2025['masc']:.2f} | Fem: R$ {g2025['fem']:.2f}")
    print(f"  Hiato (F/M): {g2025['hiato_pct']:.2f}% | Gap (1 - F/M): {g2025['gap_pct']:.2f}%")
    print('Geral (todo o dataset):')
    print(f"  Salário médio Masc: R$ {gall['masc']:.2f} | Fem: R$ {gall['fem']:.2f}")
    print(f"  Hiato (F/M): {gall['hiato_pct']:.2f}% | Gap (1 - F/M): {gall['gap_pct']:.2f}%")


if __name__ == '__main__':
    path = gerar_json()
    imprimir_disparidades()
    print(f'Arquivo JSON salvo em: {path}')
