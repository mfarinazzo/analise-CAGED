import sqlite3
from pathlib import Path
from time import perf_counter

import pandas as pd
import pyarrow.parquet as pq

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / 'projeto_caged.db'
AGG_PATH = BASE_DIR / 'output_final' / 'dados_agregados.parquet'


def carregar_em_chunks_para_sql(conn: sqlite3.Connection, table_name: str = 'dados_agregados') -> None:
    """
    Lê o Parquet (que contém SOMAS) e o carrega no SQLite.
    """
    if not AGG_PATH.exists():
        raise FileNotFoundError(
            f'Arquivo {AGG_PATH} não encontrado. Execute processador_agregado.py antes.'
        )
    pf = pq.ParquetFile(AGG_PATH)

    try:
        total_rows_meta = pf.metadata.num_rows
    except Exception:
        total_rows_meta = None

    # Ajustes de desempenho no SQLite
    conn.execute('PRAGMA journal_mode=WAL;')
    conn.execute('PRAGMA synchronous=OFF;')
    conn.execute('PRAGMA temp_store=MEMORY;')
    conn.execute('PRAGMA cache_size=-100000;')  # ~100MB

    # Limpa a tabela de destino
    print(f'>> Preparando tabela {table_name!r} no banco {DB_PATH.name!r}...')
    conn.execute(f'DROP TABLE IF EXISTS {table_name}')
    conn.commit()

    print(
        f'>> Iniciando carga do Parquet: {AGG_PATH.name}' +
        (f" | linhas totais (meta): {total_rows_meta:,}" if total_rows_meta is not None else '')
    )

    inicio = perf_counter()
    carregadas = 0
    
    # Itera sobre os "row groups" do Parquet para eficiência de memória
    for i in range(pf.num_row_groups):
        df = pf.read_row_group(i).to_pandas()
        df.to_sql(table_name, conn, if_exists='append', index=False, chunksize=5000)

        carregadas += len(df)
        decorrido = perf_counter() - inicio
        if total_rows_meta:
            pct = (carregadas / total_rows_meta) * 100
            print(f"   - RG {i+1}/{pf.num_row_groups}: +{len(df):,} linhas (acum {carregadas:,}/{total_rows_meta:,} | {pct:5.1f}%) em {decorrido:0.1f}s")
        else:
            print(f"   - RG {i+1}/{pf.num_row_groups}: +{len(df):,} linhas (acum {carregadas:,}) em {decorrido:0.1f}s")

    print(f'>> Concluído: {carregadas:,} linhas carregadas em {table_name!r} em {perf_counter()-inicio:0.1f}s')


def criar_tabelas_modelos(conn: sqlite3.Connection) -> None:
    """
    (CORRIGIDO v8 - NOVA METODOLOGIA)
    Cria a tabela 'projecoes_salariais' para armazenar
    as projeções de salário de CADA GRUPO (Homem, Mulher, etc).
    """
    print('>> Garantindo tabelas de modelos...')
    conn.execute('DROP TABLE IF EXISTS projecoes_salariais') # Garante schema limpo
    conn.execute(
        'CREATE TABLE projecoes_salariais ('
        'filtro_tipo TEXT,'          # ex: 'geral'
        'filtro_valor TEXT,'         # ex: 'brasil'
        'grupo_tipo TEXT,'           # ex: 'genero' ou 'raca_cor'
        'grupo_valor TEXT,'          # ex: 'MASC' ou 'FEM'
        'data DATE,'
        'salario_real REAL,'
        'salario_projetado REAL,'
        'salario_projetado_low REAL,'
        'salario_projetado_high REAL'
        ')'
    )
    conn.execute(
        'CREATE INDEX IF NOT EXISTS idx_projecoes_salariais '
        'ON projecoes_salariais (filtro_tipo, filtro_valor, grupo_tipo)'
    )
    
    conn.execute('DROP TABLE IF EXISTS coeficientes_regressao') # Garante schema limpo
    conn.execute(
        'CREATE TABLE coeficientes_regressao ('
        'variavel TEXT PRIMARY KEY,'
        'coeficiente REAL,'
        'p_valor REAL'
        ')'
    )
    print('>> Tabelas prontas.')


def popular_banco() -> None:
    print(f'==> Criando/atualizando banco: {DB_PATH}')
    with sqlite3.connect(DB_PATH) as conn:
        criar_tabelas_modelos(conn)
        carregar_em_chunks_para_sql(conn, 'dados_agregados')
        
    print('==> Finalizado.')


def main() -> None:
    popular_banco()


if __name__ == '__main__':
    main()