import os
import re
import unicodedata
from pathlib import Path

import pandas as pd
import py7zr

BASE_DIR = Path(__file__).parent
log_file = str(BASE_DIR / 'log_ConverterCSV.txt')


def registrar_log(mensagem: str) -> None:
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(mensagem + '\n')


RAW_FOLDER = BASE_DIR / 'CAGEDMOV_downloads'
OUTPUT_FOLDER = BASE_DIR / 'CAGED_limpos'

REQUIRED_CANONICAL = [
    'competênciamov',
    'município',
    'cnae20subclasse',
    'cbo2002ocupação',
    'graudeinstrução',
    'idade',
    'raça/cor',
    'sexo',
    'salário',
    'tipomovimentação',
]

OPTIONAL_CANONICAL = [
    'saldomovimentação',
    # Nova coluna (opcional, pois alguns meses/arquivos podem não trazer)
    'tipodedeficiência',
]

ALIASES = {
    'competenciamov': 'competênciamov',
    'competenciamovimento': 'competênciamov',
    'competencia': 'competênciamov',
    'municipio': 'município',
    'codmunicipio': 'município',
    'cod_municipio': 'município',
    'cnae20subclasse': 'cnae20subclasse',
    'subclasse': 'cnae20subclasse',
    'cbo2002ocupacao': 'cbo2002ocupação',
    'cbo': 'cbo2002ocupação',
    'graudeinstrucao': 'graudeinstrução',
    'grau_de_instrucao': 'graudeinstrução',
    'idade': 'idade',
    'racacor': 'raça/cor',
    'raca_cor': 'raça/cor',
    'raca': 'raça/cor',
    'sexo': 'sexo',
    'salario': 'salário',
    'salariomensal': 'salário',
    'valorsalario': 'salário',
    'valor_salario': 'salário',
    'salariofixo': 'salário',
    'tipomovimentacao': 'tipomovimentação',
    'tipo_movimentacao': 'tipomovimentação',
    'saldomovimentacao': 'saldomovimentação',
    'tipodedeficiencia': 'tipodedeficiência',
    'tipo_deficiencia': 'tipodedeficiência',
    'tipodeficiencia': 'tipodedeficiência',
    'deficiencia': 'tipodedeficiência',
    'pcd': 'tipodedeficiência',
}
RENAME_MAP = {
    'competênciamov': 'competencia_mov',
    'município': 'municipio',
    'cnae20subclasse': 'cnae20subclasse',
    'cbo2002ocupação': 'cbo2002ocupacao',
    'graudeinstrução': 'grau_instrucao',
    'idade': 'idade',
    'raça/cor': 'raca_cor',
    'sexo': 'genero',
    'salário': 'salario',
    'tipomovimentação': 'tipo_movimentacao',
    'tipodedeficiência': 'tipodedeficiência',
}


def extrair_arquivos() -> None:
    if not RAW_FOLDER.exists():
        registrar_log('Não foi possivel encontrar a pasta CAGEDMOV_downloads')
        return

    for arquivo in RAW_FOLDER.glob('*.7z'):
        try:
            with py7zr.SevenZipFile(arquivo, mode='r') as z:
                z.extractall(path=RAW_FOLDER)
            arquivo.unlink()
            registrar_log(f'Arquivo {arquivo.name} extraído com sucesso')
        except Exception as exc:  # noqa: BLE001
            registrar_log(f'Erro ao extrair {arquivo.name}: {exc}')


def limpar_salario(coluna: pd.Series) -> pd.Series:
    return (
        coluna.astype(str)
        .str.replace('.', '', regex=False)
        .str.replace(',', '.', regex=False)
        .str.strip()
    )


def normalizar_texto(s: str) -> str:
    s = s.strip().lower()
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r'[^a-z0-9_]+', '', s)
    return s


def construir_mapa_colunas(colunas_originais) -> dict:
    normalizadas = {normalizar_texto(c): c for c in colunas_originais}
    mapa: dict[str, str] = {}
    for canon in REQUIRED_CANONICAL:
        canon_norm = normalizar_texto(canon)
        if canon_norm in normalizadas and canon not in mapa:
            mapa[canon] = normalizadas[canon_norm]
    for alias_norm, canon in ALIASES.items():
        if alias_norm in normalizadas and canon not in mapa:
            mapa[canon] = normalizadas[alias_norm]
    return mapa


def processar_txt(txt_path: Path) -> None:
    def tentar_ler(enc: str):
        return pd.read_csv(
            txt_path,
            sep=';',
            dtype=str,
            encoding=enc,
            engine='python',
            on_bad_lines='skip',
        )

    df = None
    for enc in ('utf-8', 'cp1252', 'latin-1'):
        try:
            tmp = tentar_ler(enc)
            cols_norm = {normalizar_texto(c) for c in tmp.columns}
            requisitos = {'competenciamov', 'municipio', 'subclasse', 'cbo2002ocupacao', 'graudeinstrucao', 'racacor', 'tipomovimentacao'}
            if requisitos.issubset(cols_norm) or len(requisitos.intersection(cols_norm)) >= 5:
                df = tmp
                break
        except Exception:
            continue
    if df is None:
        registrar_log(f'Erro ao ler {txt_path.name}: falha em todas codificações (utf-8, cp1252, latin-1)')
        return

    df.columns = [col.strip() for col in df.columns]
    mapa_colunas = construir_mapa_colunas(list(df.columns))
    faltantes = [c for c in REQUIRED_CANONICAL if c not in mapa_colunas]
    if faltantes:
        disponiveis_norm = ', '.join(sorted(df.columns))
        registrar_log(
            f'Colunas ausentes em {txt_path.name}: {", ".join(faltantes)}'
        )
        registrar_log(f'Colunas encontradas: {disponiveis_norm}')
        return

    select_cols_src = [mapa_colunas[c] for c in REQUIRED_CANONICAL]
    select_cols_dst = REQUIRED_CANONICAL.copy()
    for opt in OPTIONAL_CANONICAL:
        if opt in mapa_colunas:
            select_cols_src.append(mapa_colunas[opt])
            select_cols_dst.append(opt)
    df = df[select_cols_src].copy()
    df.columns = select_cols_dst

    required_now = ['competênciamov','município','cnae20subclasse','cbo2002ocupação','graudeinstrução','idade','raça/cor','sexo','salário','tipomovimentação']
    if not df.empty:
        ultima = df.iloc[-1]
        try:
            if any(pd.isna(ultima[col]) or str(ultima[col]).strip()=='' for col in required_now):
                df = df.iloc[:-1].copy()
        except Exception:
            pass

    col_tip = df['tipomovimentação'].astype(str).str.strip()
    tipomov_num = pd.to_numeric(col_tip, errors='coerce')
    mask_tip_num = tipomov_num.isin([10, 20, 25])

    col_tip_txt = (
        col_tip.str.lower()
        .str.normalize('NFKD')
        .str.encode('ascii', errors='ignore')
        .str.decode('ascii')
    )
    mask_tip_txt = col_tip_txt.str.contains('admi')

    mask_saldo = pd.Series([False] * len(df))
    if 'saldomovimentação' in df.columns:
        saldo_num = pd.to_numeric(df['saldomovimentação'].astype(str).str.strip(), errors='coerce')
        mask_saldo = saldo_num == 1

    df = df[mask_tip_num | mask_tip_txt | mask_saldo]

    if df.empty:
        registrar_log(f'Nenhuma movimentação válida em {txt_path.name}')
        return

    df['salário'] = limpar_salario(df['salário'])
    df['salário'] = pd.to_numeric(df['salário'], errors='coerce')
    df['idade'] = pd.to_numeric(df['idade'], errors='coerce').astype('Int64')

    # Mantém apenas linhas com salário e idade válidos (sem impor cortes de outliers aqui)
    # Os cortes/outliers serão tratados na etapa de agregação para não afetar a contagem bruta de admissões.
    df = df.dropna(subset=['salário', 'idade'])

    df = df.rename(columns=RENAME_MAP)
    df['competencia_mov'] = (
        df['competencia_mov']
        .astype(str)
        .str.strip()
        .str.replace(r'[^0-9]', '', regex=True)
        .str.zfill(6)
    )
    df['genero'] = df['genero'].str.strip().str.upper()
    df['raca_cor'] = df['raca_cor'].str.strip().str.upper()
    df['grau_instrucao'] = df['grau_instrucao'].str.strip().str.upper()
    df['municipio'] = df['municipio'].str.strip()
    df['cnae20subclasse'] = df['cnae20subclasse'].str.strip()
    df['cbo2002ocupacao'] = df['cbo2002ocupacao'].str.strip()

    competencia = df['competencia_mov'].iloc[0] if not df.empty else txt_path.stem
    competencia = str(competencia).strip()
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_FOLDER / f'CAGEDMOV_limpo_{competencia}.csv'

    df.to_csv(output_path, index=False)
    registrar_log(f'Arquivo limpo gerado: {output_path.name}')


def main() -> None:
    extrair_arquivos()

    if not RAW_FOLDER.exists():
        return

    # Permite testes com subconjunto de meses via variável de ambiente: IPP_TEST_MESES="202105,202106,202203"
    meses_env = os.environ.get('IPP_TEST_MESES', '').strip()
    meses_filtro = [m.strip() for m in meses_env.split(',') if m.strip().isdigit()] if meses_env else []

    arquivos = list(RAW_FOLDER.glob('*.txt'))
    if meses_filtro:
        arquivos = [p for p in arquivos if any(m in p.stem for m in meses_filtro)]

    for txt_path in sorted(arquivos):
        processar_txt(txt_path)


if __name__ == '__main__':
    main()