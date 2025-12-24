from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import pandas as pd

from .common import connect, save_json

COL_DEF = 'tipodedeficiência'

DEF_MAP: Dict[str, str] = {
    '0': 'Não Deficiente',
    '1': 'Física',
    '2': 'Auditiva',
    '3': 'Visual',
    '4': 'Intelectual (Mental)',
    '5': 'Múltipla',
    '6': 'Reabilitado',
    '9': 'Não Identificado',
}

DEF_ORDEM = list(DEF_MAP.keys())


def gerar_json() -> Path:
    with connect() as _conn:
        cols = [r[1] for r in _conn.execute("PRAGMA table_info('dados_agregados')").fetchall()]
    if COL_DEF not in cols:
        payload = {
            'title': 'Salário Médio de Admissão por Tipo de Deficiência',
            'subtitle': 'Coluna tipodedeficiência ausente em dados_agregados.',
            'yAxisTitle': 'R$ (média)',
            'categories': [],
            'series': [],
            'chart': { 'seriesType': 'column' },
        }
        return save_json('deficiencia', payload)

    sql = f"""
        SELECT data,
               "{COL_DEF}" AS deficiencia,
               SUM(soma_salario)*1.0 / NULLIF(SUM(total_admissoes), 0) AS salario_medio
        FROM dados_agregados
        WHERE total_admissoes > 0
        GROUP BY data, "{COL_DEF}"
        ORDER BY data
    """

    with connect() as conn:
        df = pd.read_sql(sql, conn)

    if df.empty:
        payload = {
            'title': 'Salário Médio de Admissão por Tipo de Deficiência',
            'subtitle': 'Sem dados disponíveis',
            'yAxisTitle': 'R$ (média)',
            'categories': [],
            'series': [],
            'chart': { 'seriesType': 'column' },
        }
        return save_json('deficiencia', payload)

    df['data'] = pd.to_datetime(df['data'])
    df['deficiencia'] = df['deficiencia'].astype(str)
    df = df.sort_values(['data', 'deficiencia'])

    pivot = df.pivot(index='data', columns='deficiencia', values='salario_medio')
    categorias = [d.strftime('%Y-%m') for d in pivot.index]

    palette = ['#4e79a7','#f28e2b','#e15759','#76b7b2','#59a14f','#edc948','#b07aa1','#9c755f']

    series: List[Dict] = []
    for i, cod in enumerate(DEF_ORDEM):
        nome = DEF_MAP.get(cod, f'Código {cod}')
        if cod in pivot.columns:
            col = pivot[cod].round(2)
            vals = col.where(col.notna(), None).tolist()
        else:
            vals = [None] * len(categorias)
        series.append({
            'type': 'column',
            'name': nome,
            'data': vals,
            'color': palette[i % len(palette)],
        })

    payload = {
        'title': 'Salário Médio de Admissão por Tipo de Deficiência',
        'subtitle': 'Fonte: projeto_caged.db (dados_agregados)',
        'yAxisTitle': 'R$ (média)',
        'categories': categorias,
        'series': series,
        'chart': { 'seriesType': 'column' },
    }
    return save_json('deficiencia', payload)


def imprimir_resumo(ano: str = '2025') -> None:
    with connect() as conn:
        df = pd.read_sql(
            f"""
            SELECT "{COL_DEF}" AS deficiencia,
                   SUM(soma_salario)*1.0 / NULLIF(SUM(total_admissoes), 0) AS media
            FROM dados_agregados
            WHERE data LIKE ? AND total_admissoes > 0
            GROUP BY "{COL_DEF}"
            ORDER BY 1
            """,
            conn,
            params=[f"{ano}-%"],
        )

    print(f'=== Salário Médio por Tipo de Deficiência — {ano} ===')
    for _, row in df.iterrows():
        cod = str(row['deficiencia'])
        nome = DEF_MAP.get(cod, f'Código {cod}')
        try:
            val = float(row['media'])
            print(f"  {nome:<22} | R$ {val:,.2f}")
        except Exception:
            print(f"  {nome:<22} | (sem dados)")


if __name__ == '__main__':
    p = gerar_json()
    imprimir_resumo()
    print(f'Arquivo JSON salvo em: {p}')
