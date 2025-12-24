from pathlib import Path
import os
import sqlite3
import sys
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


BASE_DIR = Path(__file__).parent
CLEAN_FOLDER = BASE_DIR / 'CAGED_limpos'
OUTPUT_FOLDER = BASE_DIR / 'output_final'
STAGING_DIR = OUTPUT_FOLDER / 'tmp_aggs'
STAGING_DB = STAGING_DIR / 'aggs_temp.db'
OUTPUT_FILE = OUTPUT_FOLDER / 'dados_agregados.parquet'

AGG_KEYS = [
    'data',
    'municipio',
    'cnae20subclasse',
    'grau_instrucao',
    'genero',
    'raca_cor',
    'tipodedeficiência',  # NOVO
]


def _prepare_sqlite(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute('PRAGMA journal_mode = WAL;')
    cur.execute('PRAGMA synchronous = OFF;')
    cur.execute('PRAGMA temp_store = MEMORY;')
    cur.execute('PRAGMA cache_size = -100000;')  # ~100MB cache
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS aggs (
            data TEXT,
            municipio TEXT,
            cnae20subclasse TEXT,
            grau_instrucao TEXT,
            genero TEXT,
            raca_cor TEXT,
            "tipodedeficiência" TEXT,
            sum_salario REAL NOT NULL,
            sum_idade REAL NOT NULL,
            n INTEGER NOT NULL,
            PRIMARY KEY ({', '.join(AGG_KEYS)})
        )
        """
    )
    conn.commit()


def _merge_chunk_into_sqlite(conn: sqlite3.Connection, df_chunk_grp: pd.DataFrame) -> None:
    if df_chunk_grp.empty:
        return
    df_chunk_grp = df_chunk_grp.reset_index()
    sql = (
        'INSERT INTO aggs ('
        + ', '.join(AGG_KEYS)
        + ', sum_salario, sum_idade, n) VALUES ('
        + ', '.join(['?'] * (len(AGG_KEYS) + 3))
        + ') ON CONFLICT('
        + ', '.join(AGG_KEYS)
        + ') DO UPDATE SET '
        'sum_salario = sum_salario + excluded.sum_salario, '
        'sum_idade = sum_idade + excluded.sum_idade, '
        'n = n + excluded.n'
    )
    cols = AGG_KEYS + ['sum_salario', 'sum_idade', 'n']
    records = list(df_chunk_grp[cols].itertuples(index=False, name=None))
    conn.executemany(sql, records)


def agregar_arquivo(csv_path: Path, conn: sqlite3.Connection, chunksize: int = 500_000) -> None:
    start_file = time.time()
    print(f'Iniciando: {csv_path.name}')
    def _detectar_col_deficiencia(path: Path) -> str | None:
        import csv
        try:
            with path.open('r', encoding='utf-8', errors='ignore') as f:
                reader = csv.reader(f)
                header = next(reader)
                header_norm = [h.strip().lower() for h in header]
        except Exception:
            return None
        candidates = ['tipodedeficiência', 'tipodedeficiencia', 'tipo_deficiencia', 'deficiencia']
        for cand in candidates:
            if cand.lower() in header_norm:
                # retorna o nome como aparece no arquivo original
                return header[header_norm.index(cand.lower())]
        return None

    col_def_src = _detectar_col_deficiencia(csv_path)

    usecols = [
        'competencia_mov',
        'municipio',
        'cnae20subclasse',
        'grau_instrucao',
        'genero',
        'raca_cor',
        'salario',
        'idade',
    ] + ([col_def_src] if col_def_src else [])

    chunk_idx = 0
    total_rows_in = 0
    total_groups = 0
    for chunk in pd.read_csv(
        csv_path,
        usecols=usecols,
        dtype={
            'competencia_mov': str,
            'municipio': str,
            'cnae20subclasse': str,
            # 'cbo2002ocupacao': str, # REMOVIDO
            'grau_instrucao': str,
            'genero': str,
            'raca_cor': str,
        },
        chunksize=chunksize,
        engine='python',
        on_bad_lines='skip',
    ):
        chunk_idx += 1
        rows_in = len(chunk)
        total_rows_in += rows_in

        chunk['competencia_mov'] = chunk['competencia_mov'].astype(str).str.strip()
        chunk['data'] = (
            pd.to_datetime(chunk['competencia_mov'], format='%Y%m', errors='coerce')
            .dt.strftime('%Y-%m')
        )
        # Normaliza e converte idade/salário, aceitando formatos com vírgula
        chunk['idade'] = pd.to_numeric(chunk['idade'], errors='coerce')
        if chunk['salario'].dtype.kind in {'O', 'U'}:
            chunk['salario'] = (
                chunk['salario'].astype(str)
                .str.replace('.', '', regex=False)
                .str.replace(',', '.', regex=False)
            )
        chunk['salario'] = pd.to_numeric(chunk['salario'], errors='coerce')
        chunk = chunk.dropna(subset=['data', 'salario', 'idade'])

        # Filtros de qualidade / outliers básicos
        chunk = chunk[(chunk['idade'] >= 14) & (chunk['idade'] <= 80)]
        chunk = chunk[(chunk['salario'] > 0) & (chunk['salario'] < 200_000)]

        if col_def_src and col_def_src in chunk.columns:
            chunk['tipodedeficiência'] = chunk[col_def_src].astype(str).str.strip()
        else:
            chunk['tipodedeficiência'] = '9'

        key_cols = ['municipio', 'cnae20subclasse', 'grau_instrucao', 'genero', 'raca_cor', 'tipodedeficiência']
        for col in key_cols:
            chunk[col] = chunk[col].astype(str).str.strip()

        grp = chunk.groupby(AGG_KEYS, dropna=False).agg(
            sum_salario=('salario', 'sum'),
            sum_idade=('idade', 'sum'),
            n=('salario', 'count'),
        )
        total_groups += len(grp)

        _merge_chunk_into_sqlite(conn, grp)
        print(f'   Chunk {chunk_idx:>3}: linhas={rows_in:,} grupos={len(grp):,}')
        del chunk
        del grp

    print(
        f'Concluído: {csv_path.name} | linhas totais={total_rows_in:,} grupos agregados~={total_groups:,} | tempo={time.time()-start_file:,.1f}s'
    )


def processar_incremental() -> None:
    # Permite testes com subconjunto de meses via variável de ambiente: IPP_TEST_MESES="202105,202106,202203"
    meses_env = os.environ.get('IPP_TEST_MESES', '').strip()
    meses_filtro = [m.strip() for m in meses_env.split(',') if m.strip().isdigit()] if meses_env else []

    arquivos_csv = sorted(CLEAN_FOLDER.glob('*.csv'))
    if meses_filtro:
        arquivos_csv = [p for p in arquivos_csv if any(m in p.stem for m in meses_filtro)]
    if not arquivos_csv:
        raise FileNotFoundError('Nenhum arquivo limpo encontrado em CAGED_limpos/.')

    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    if STAGING_DB.exists():
        STAGING_DB.unlink()

    with sqlite3.connect(STAGING_DB) as conn:
        _prepare_sqlite(conn)

        total_files = len(arquivos_csv)
        print(f'Total de arquivos a processar: {total_files}')
        for i, csv_path in enumerate(arquivos_csv, start=1):
            print(f'[{i}/{total_files}] {csv_path.name}')
            conn.execute('BEGIN')
            agregar_arquivo(csv_path, conn)
            conn.execute('COMMIT')

    return None

def exportar_streaming_para_parquet() -> None:
    """
    (CORRIGIDO v6)
    Exporta os dados agregados (SOMAS) para um arquivo Parquet final.
    Não calcula mais as médias aqui.
    """
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(STAGING_DB) as conn:
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) FROM aggs')
        total = cur.fetchone()[0] or 0
        if total == 0:
            # Schema final com SOMAS
            df_vazio = pd.DataFrame(columns=AGG_KEYS + ['soma_salario', 'soma_idade', 'total_admissoes'])
            table = pa.Table.from_pandas(df_vazio, preserve_index=False)
            pq.write_table(table, OUTPUT_FILE)
            print('Nenhuma linha agregada. Parquet vazio gerado.')
            return

        cols = ', '.join(AGG_KEYS)
        query = f'SELECT {cols}, sum_salario, sum_idade, n FROM aggs'
        cur.execute(query)

        writer = None
        processed = 0
        batch_size = 100_000
        t0 = time.time()
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            
            # Renomeia 'n' para 'total_admissoes'
            df = pd.DataFrame(rows, columns=AGG_KEYS + ['soma_salario', 'soma_idade', 'total_admissoes'])
            
            # Garante a ordem correta das colunas
            df = df[AGG_KEYS + ['soma_salario', 'soma_idade', 'total_admissoes']]

            table = pa.Table.from_pandas(df, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(OUTPUT_FILE, table.schema, compression='snappy')
            writer.write_table(table)

            processed += len(df)
            perc = (processed / total) * 100 if total else 100
            print(f'   Exportados {processed:,}/{total:,} ({perc:5.1f}%) em {time.time()-t0:,.1f}s')

        if writer is not None:
            writer.close()
        print(f'Parquet salvo em: {OUTPUT_FILE} | linhas: {processed:,} | tempo: {time.time()-t0:,.1f}s')


def main() -> None:
    t0 = time.time()
    processar_incremental()
    exportar_streaming_para_parquet()
    # Limpa o banco de dados temporário
    if STAGING_DB.exists():
        try:
            STAGING_DB.unlink()
            print(f'Banco temporário {STAGING_DB.name} removido.')
        except PermissionError:
            # Em alguns ambientes Windows o arquivo pode ficar travado por antivirus/AV/FS cache
            print(f'Aviso: não foi possível remover {STAGING_DB.name} agora (em uso). Prosseguindo mesmo assim.')
    print(f'Tempo total: {time.time()-t0:,.1f}s')


if __name__ == '__main__':
    main()