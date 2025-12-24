from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from .common import connect, save_json


# Mapeamento básico (padrão CAGED/eSocial). Caso algum código não exista aqui, cai no rótulo "Código X".
ESC_MAP: Dict[str, str] = {
    '1': 'Analfabeto',
    '2': 'Fundamental incompleto',
    '3': 'Fundamental completo',
    '4': 'Médio incompleto',
    '5': 'Médio completo',
    '6': 'Superior incompleto',
    '7': 'Superior completo',
    '8': 'Pós-graduação',
    '9': 'Mestrado',
    '10': 'Doutorado',
    '11': 'Pós-doutorado',
}

IGNORAR_ESC = {'80', '99'}  # Não identificado / Não se aplica


def _rotulo_escolaridade(code: str) -> str:
    return ESC_MAP.get(code, f"Código {code}")


def gerar_json() -> Path:
    # Usaremos colunas de contagem (total_admissoes) para empilhamento percentual.
    # Aqui NÃO separamos por sexo; o foco é apenas a escolaridade.
    sql = f"""
        SELECT data, grau_instrucao, SUM(total_admissoes) AS total
        FROM dados_agregados
        WHERE total_admissoes > 0
          AND grau_instrucao NOT IN ({','.join([f"'{c}'" for c in IGNORAR_ESC])})
        GROUP BY data, grau_instrucao
        ORDER BY data
    """
    with connect() as conn:
        df = pd.read_sql(sql, conn)

    if df.empty:
        payload = {
            'title': 'Admissões por Escolaridade (colunas empilhadas)',
            'subtitle': 'Sem dados disponíveis',
            'yAxisTitle': 'Participação (%)',
            'categories': [],
            'series': [],
            'chart': { 'seriesType': 'column', 'stacking': 'percent', 'grouping': False, 'tooltipMode': 'percent-with-abs' },
        }
        return save_json('escolaridade', payload)

    df['data'] = pd.to_datetime(df['data'])
    df['grau_instrucao'] = df['grau_instrucao'].astype(str)
    df = df.sort_values(['data', 'grau_instrucao'])

    # Categorias do eixo X
    categorias = sorted(df['data'].dt.strftime('%Y-%m').unique().tolist())

    # Ordem estável de escolaridade e gênero
    esc_ordem = sorted([c for c in df['grau_instrucao'].unique().tolist() if c not in IGNORAR_ESC], key=lambda x: (len(x), x))

    # Tabela para buscar valores rapidamente
    # index: (data_str, esc) -> total
    df['data_str'] = df['data'].dt.strftime('%Y-%m')
    key_cols = ['data_str', 'grau_instrucao']
    lookup = df.set_index(key_cols)['total'].to_dict()

    # Monta séries no formato Highcharts: uma coluna empilhada por mês (sem separar por sexo).
    series: List[Dict] = []

    # Paleta ampla (Tableau/ColorBrewer-like)
    palette = ['#4e79a7','#f28e2b','#e15759','#76b7b2','#59a14f','#edc948','#b07aa1','#ff9da7','#9c755f','#bab0ac']

    for esc_idx, esc in enumerate(esc_ordem):
        name = _rotulo_escolaridade(esc)
        color = palette[esc_idx % len(palette)]
        data_vals = []
        for cat in categorias:
            v = lookup.get((cat, esc), 0)
            data_vals.append(int(v))  # 0 para ausência
        series.append({
            'type': 'column',
            'name': name,
            'data': data_vals,
            'color': color,
        })

    payload = {
        'title': 'Admissões por Escolaridade (colunas empilhadas)',
        'subtitle': 'Fonte: projeto_caged.db (dados_agregados) — Excluídos: Não identificado/Não se aplica',
        'yAxisTitle': 'Participação (%)',
        'categories': categorias,
        'series': series,
        'chart': {
            'seriesType': 'column',
            'stacking': 'percent',
            'grouping': False,
            'tooltipMode': 'percent-with-abs',
        },
    }
    return save_json('escolaridade', payload)


def imprimir_resumo() -> None:
    # Mostra composição média de 2025 por escolaridade (somando ambos os sexos)
    with connect() as conn:
        df = pd.read_sql(
            f"""
            SELECT grau_instrucao, SUM(total_admissoes) AS total
            FROM dados_agregados
            WHERE data LIKE '2025-%' AND total_admissoes > 0 AND grau_instrucao NOT IN ({','.join([f"'{c}'" for c in IGNORAR_ESC])})
            GROUP BY grau_instrucao
            ORDER BY 1
            """,
            conn,
        )
    total = df['total'].sum() if not df.empty else 0
    print('=== Composição 2025 por Escolaridade (ambos os sexos) ===')
    for _, row in df.sort_values('total', ascending=False).iterrows():
        nome = _rotulo_escolaridade(str(row['grau_instrucao']))
        pct = (row['total'] / total * 100) if total else 0
        print(f"  {nome:<24} | {row['total']:>10,} | {pct:5.2f}%")


if __name__ == '__main__':
    p = gerar_json()
    imprimir_resumo()
    print(f'Arquivo JSON salvo em: {p}')
