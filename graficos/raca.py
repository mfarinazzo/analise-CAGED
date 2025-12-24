from __future__ import annotations

import pandas as pd
from pathlib import Path
from typing import Dict, List

from .common import connect, save_json


RACA_MAP = {
    '1': 'Branca',
    '2': 'Preta',
    '3': 'Parda',
    '4': 'Amarela',
    '5': 'Indígena',
}

# Paleta acessível (fundo escuro):
RACA_COLORS = {
    'Branca': '#4e79a7',     # azul
    'Preta': '#e15759',      # vermelho
    'Parda': '#f28e2b',      # laranja
    'Amarela': '#edc948',    # amarelo
    'Indígena': '#b07aa1',   # roxo
}


def gerar_json() -> Path:
    sql = """
        SELECT data, raca_cor,
               SUM(soma_salario)*1.0/SUM(total_admissoes) AS salario_medio
        FROM dados_agregados
        WHERE total_admissoes > 0
        GROUP BY data, raca_cor
        ORDER BY data
    """
    with connect() as conn:
        df = pd.read_sql(sql, conn)

    df['data'] = pd.to_datetime(df['data'])
    df['raca_cor'] = df['raca_cor'].astype(str)
    df = df.sort_values(['data', 'raca_cor'])

    pivot = df.pivot(index='data', columns='raca_cor', values='salario_medio')
    categorias = [d.strftime('%Y-%m') for d in pivot.index]

    series = []
    for cod, nome in RACA_MAP.items():
        if cod in pivot.columns:
            col = pivot[cod].round(2)
            vals = col.where(col.notna(), None).tolist()
            series.append({'name': nome, 'data': vals, 'color': RACA_COLORS.get(nome)})

    payload = {
        'title': 'Salário Médio de Admissão por Raça/Cor',
        'subtitle': 'Fonte: projeto_caged.db (dados_agregados)',
        'yAxisTitle': 'R$ (média)',
        'categories': categorias,
        'series': series,
    }
    return save_json('raca', payload)


def imprimir_disparidades() -> None:
    # Razão vs Branca (1) por período e geral
    with connect() as conn:
        df_2025 = pd.read_sql(
            """
            SELECT raca_cor, SUM(soma_salario)*1.0/SUM(total_admissoes) AS media
            FROM dados_agregados
            WHERE data LIKE '2025-%' AND total_admissoes > 0
            GROUP BY raca_cor
            """,
            conn,
        )
        df_all = pd.read_sql(
            """
            SELECT raca_cor, SUM(soma_salario)*1.0/SUM(total_admissoes) AS media
            FROM dados_agregados
            WHERE total_admissoes > 0
            GROUP BY raca_cor
            """,
            conn,
        )

    def ratios(df: pd.DataFrame) -> List[str]:
        df = df.copy()
        df['raca_cor'] = df['raca_cor'].astype(str)
        base = df.loc[df['raca_cor'] == '1', 'media']
        if base.empty or pd.isna(base.values[0]):
            return ["Base 'Branca' indisponível"]
        b = float(base.values[0])
        lines = []
        for cod, nome in RACA_MAP.items():
            mrow = df.loc[df['raca_cor'] == cod, 'media']
            if mrow.empty:
                continue
            v = float(mrow.values[0])
            r = v / b if b else float('nan')
            lines.append(f"  {nome:<16} | média R$ {v:,.2f} | vs Branca: {r*100:,.2f}% | gap: {(1-r)*100:,.2f}%")
        return lines

    print('=== Disparidade por Raça/Cor (referência: Branca) ===')
    print('2025:')
    for ln in ratios(df_2025):
        print(ln)
    print('Geral (todo o dataset):')
    for ln in ratios(df_all):
        print(ln)


if __name__ == '__main__':
    path = gerar_json()
    imprimir_disparidades()
    print(f'Arquivo JSON salvo em: {path}')
