import logging
import sqlite3
from pathlib import Path
from typing import Dict, Optional, Tuple, List

import numpy as np
import pandas as pd
import statsmodels.api as sm

try:
    from pmdarima import auto_arima as _auto_arima
    HAS_PMDARIMA = True
except Exception as _e:
    logging.warning('pmdarima indisponível (%s). Usando fallback com statsmodels.', _e)
    HAS_PMDARIMA = False


DB_PATH = Path('projeto_caged.db')
LOG_FILE_PATH = Path('modelagem_log.txt') 
QUALIDADE_CSV = Path('output_final') / 'qualidade_mensal.csv'

GENERO_MAP_SQL = {'1': 'MASC', '3': 'FEM'} 
CENARIOS_ARIMA: List[Tuple[str, str]] = [('geral', 'brasil')]
UF_MAP = {
    '11': ('RO', 'NORTE'), '12': ('AC', 'NORTE'), '13': ('AM', 'NORTE'),
    '14': ('RR', 'NORTE'), '15': ('PA', 'NORTE'), '16': ('AP', 'NORTE'),
    '17': ('TO', 'NORTE'), '21': ('MA', 'NORDESTE'), '22': ('PI', 'NORDESTE'),
    '23': ('CE', 'NORDESTE'), '24': ('RN', 'NORDESTE'), '25': ('PB', 'NORDESTE'),
    '26': ('PE', 'NORDESTE'), '27': ('AL', 'NORDESTE'), '28': ('SE', 'NORDESTE'),
    '29': ('BA', 'NORDESTE'), '31': ('MG', 'SUDESTE'), '32': ('ES', 'SUDESTE'),
    '33': ('RJ', 'SUDESTE'), '35': ('SP', 'SUDESTE'), '41': ('PR', 'SUL'),
    '42': ('SC', 'SUL'), '43': ('RS', 'SUL'), '50': ('MS', 'CENTRO-OESTE'),
    '51': ('MT', 'CENTRO-OESTE'), '52': ('GO', 'CENTRO-OESTE'),
    '53': ('DF', 'CENTRO-OESTE')
}

GRUPOS_DE_PROJECAO = {
    'genero': ['1', '3'],
    'raca_cor': ['1', '2', '3', '4', '5'],
    'grau_instrucao': ['1','2','3','4','5','6','7','8','9','10','11'],
    'tipodedeficiência': ['0','1','2','3','4','5','6','9'],
}
GRUPOS_MAP = {
    'genero': {'1': 'MASC', '3': 'FEM'},
    'raca_cor': {'1': 'BRANCA', '2': 'PRETA', '3': 'PARDA', '4': 'AMARELA', '5': 'INDIGENA'},
    'grau_instrucao': {
        '1':'ANALFABETO','2':'FUND INCOMPL','3':'FUND COMPL','4':'MED INCOMPL','5':'MED COMPL','6':'SUP INCOMPL','7':'SUP COMPL','8':'POS-GRAD','9':'MESTRADO','10':'DOUTORADO','11':'POS-DOC'
    },
    'tipodedeficiência': {
        '0':'NAO DEFICIENTE','1':'FISICA','2':'AUDITIVA','3':'VISUAL','4':'INTELECTUAL','5':'MULTIPLA','6':'REABILITADO','9':'NAO IDENTIFICADO'
    }
}

# Configuração de Logging
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s | %(levelname)s | modelagem.py] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_FILE_PATH, mode='w'),
        logging.StreamHandler()
    ]
)


def carregar_dados_para_regressao(conn: sqlite3.Connection) -> pd.DataFrame:
    """Agrega dados no SQLite para regressão."""
    logging.info('Iniciando query de agregação (server-side) para Regressão...')
    genero_sql = "CASE WHEN genero = '1' THEN 'MASC' WHEN genero = '3' THEN 'FEM' ELSE 'NA' END"
    regiao_sql = "CASE "
    for code, (uf, regiao) in UF_MAP.items():
        regiao_sql += f"WHEN SUBSTR(municipio, 1, 2) = '{code}' THEN '{regiao}' "
    regiao_sql += "ELSE 'NA' END"
    cnae_divisao_sql = "SUBSTR(cnae20subclasse, 1, 2)"

    query = f"""
        SELECT
            {genero_sql} AS genero_nome,
            raca_cor,
            {cnae_divisao_sql} AS cnae_divisao,
            grau_instrucao,
            "tipodedeficiência" AS tipo_def,
            {regiao_sql} AS regiao,
            SUM(soma_salario) / SUM(total_admissoes) as salario_medio,
            SUM(soma_idade) / SUM(total_admissoes) as idade_media
        FROM dados_agregados
        WHERE total_admissoes > 0 AND soma_salario > 0
        GROUP BY genero_nome, raca_cor, cnae_divisao, grau_instrucao, tipo_def, regiao
    """
    df_reg = pd.read_sql(query, conn)
    valid_raca = {'1','2','3','4','5','6','9'}
    df_reg['raca_cor'] = df_reg['raca_cor'].astype(str)
    df_reg.loc[~df_reg['raca_cor'].isin(valid_raca), 'raca_cor'] = '9'  # 'Não Identificado'
    logging.info(f'Agregação da Regressão concluída. {len(df_reg)} grupos carregados.')
    return df_reg


def rodar_regressao(df_reg: pd.DataFrame, conn: sqlite3.Connection) -> None:
    """Ajusta OLS log-linear e grava coeficientes."""
    logging.info('Iniciando modelo de Regressão...')
    if df_reg.empty:
        raise ValueError('DataFrame de regressão está vazio.')

    df_reg['log_salario_medio'] = np.log(df_reg['salario_medio'])
    categorias = ['genero_nome', 'raca_cor', 'cnae_divisao', 'grau_instrucao', 'tipo_def', 'regiao']
    dummies_list = []
    for coluna in categorias:
        df_reg[coluna] = df_reg[coluna].astype(str).str.upper().fillna('NA')
        dummies_list.append(coluna)

    dummies = pd.get_dummies(df_reg[dummies_list], prefix=dummies_list, drop_first=True, dtype=np.int8)
    X_parts = [dummies, df_reg['idade_media'].fillna(0)]
    X = pd.concat(X_parts, axis=1)
    X = sm.add_constant(X, has_constant='add')
    Y = df_reg['log_salario_medio']

    logging.info(f'Treinando modelo OLS com {len(X.columns)} variáveis...')
    modelo = sm.OLS(Y, X).fit()
    resultados = pd.DataFrame({
        'variavel': modelo.params.index,
        'coeficiente': modelo.params.values,
        'p_valor': modelo.pvalues.values,
    })
    resultados.to_sql('coeficientes_regressao', conn, if_exists='replace', index=False)
    logging.info('Tabela coeficientes_regressao (Diagnóstico) atualizada.')


def carregar_dados_para_arima(conn: sqlite3.Connection, filtro_tipo: str, filtro_valor: str, grupo_coluna_sql: str, grupos_sql: List[str]) -> pd.DataFrame:
    """Filtra e agrega dados no SQLite para um cenário e um tipo de grupo."""
    log_ctx = f'SARIMA ({filtro_tipo}={filtro_valor}, Grupo={grupo_coluna_sql})'
    logging.info('[%s] Iniciando query de agregação (server-side)...', log_ctx)

    quoted_group = f'"{grupo_coluna_sql}"' if any(ch in grupo_coluna_sql for ch in ' áàãâéêíóôõúçÁÀÃÂÉÊÍÓÔÕÚÇ') else grupo_coluna_sql
    where_clauses = [f"{quoted_group} IN ({','.join(['?'] * len(grupos_sql))})", "total_admissoes > 0", "soma_salario > 0"]
    params = grupos_sql # Começa com os parâmetros do GRUPO

    if filtro_tipo == 'municipio':
        where_clauses.append("municipio = ?")
        params.append(filtro_valor)
    elif filtro_tipo == 'regiao':
        regiao_sql = "CASE "
        for code, (uf, regiao) in UF_MAP.items():
            regiao_sql += f"WHEN SUBSTR(municipio, 1, 2) = '{code}' THEN '{regiao}' "
        regiao_sql += "ELSE 'NA' END"
        where_clauses.append(f"({regiao_sql}) = ?")
        params.append(filtro_valor)
    elif filtro_tipo == 'cnae':
        where_clauses.append("cnae20subclasse LIKE ?")
        params.append(f"{filtro_valor}%")

    query = f"""
        SELECT
            data,
            {quoted_group} as grupo_valor,
            SUM(soma_salario) as soma_salario_total,
            SUM(total_admissoes) as total_admissoes_total
        FROM
            dados_agregados
        WHERE
            {' AND '.join(where_clauses)}
        GROUP BY
            data, {quoted_group}
        ORDER BY
            data
    """
    
    df_agg = pd.read_sql(query, conn, params=params)
    return df_agg


def calcular_serie_salarial(df_agg_grupo: pd.DataFrame) -> pd.Series:
    """Calcula a série temporal de salário para um grupo."""
    if df_agg_grupo.empty:
        return pd.Series(dtype=float)

    # Dados já estão agregados, só precisamos calcular a média
    df_agg_grupo = df_agg_grupo[df_agg_grupo['total_admissoes_total'] > 0].copy()
    if df_agg_grupo.empty:
        return pd.Series(dtype=float)

    df_agg_grupo['media_salario'] = (
        df_agg_grupo['soma_salario_total'] / df_agg_grupo['total_admissoes_total']
    )
    try:
        if QUALIDADE_CSV.exists():
            q = pd.read_csv(QUALIDADE_CSV, sep=';')
            q['data'] = q['mes']
            df_agg_grupo = df_agg_grupo.merge(q[['data','peso_qualidade']], on='data', how='left')
            df_agg_grupo = df_agg_grupo[(df_agg_grupo['peso_qualidade'].fillna(1.0) >= 0.6)]
    except Exception as _e:
        logging.warning('Falha ao aplicar pesos de qualidade: %s', _e)
    
    # Define o 'data' como índice para a série temporal
    serie = df_agg_grupo.set_index('data')['media_salario']
    serie.index = pd.PeriodIndex(serie.index, freq='M')
    return serie


def _auto_arima_fallback(serie: pd.Series, seasonal: bool = False, m: int = 1):
    import warnings
    from statsmodels.tsa.statespace.sarimax import SARIMAX
    warnings.filterwarnings('ignore')
    y = serie.astype(float)
    y.index = pd.period_range(start=y.index[0].start_time, periods=len(y), freq='M')
    best_aic = np.inf
    best_res = None
    ps = range(0, 3); qs = range(0, 3); ds = range(0, 2)
    for p in ps:
        for d in ds:
            for q in qs:
                try:
                    model = SARIMAX(y, order=(p, d, q), enforce_stationarity=False, enforce_invertibility=False)
                    res = model.fit(disp=False)
                    if res.aic < best_aic: best_aic, best_res = res.aic, res
                    if seasonal and m > 1:
                        model_s = SARIMAX(y, order=(p, d, q), seasonal_order=(1, 0, 1, m), enforce_stationarity=False, enforce_invertibility=False)
                        res_s = model_s.fit(disp=False)
                        if res_s.aic < best_aic: best_aic, best_res = res_s.aic, res_s
                except Exception: continue
    if best_res is None: raise RuntimeError('Falha no ajuste SARIMA fallback.')
    return best_res


def executar_projecao_salarial(
    conn: sqlite3.Connection,
    filtro_tipo: str,
    filtro_valor: str,
    grupo_coluna_sql: str,
    grupo_codigo: str,
    grupo_nome: str,
    serie_salarial: pd.Series,
    periodos: int = 60,
    alpha: float = 0.05,
) -> None:
    """Ajusta SARIMA sazonal para uma série salarial e salva resultados no banco."""
    log_ctx = f'SARIMA ({filtro_tipo}={filtro_valor}, Grupo={grupo_nome})'
    
    if serie_salarial.empty or len(serie_salarial) < 24:
        logging.warning('[%s] Dados insuficientes para série temporal sazonal (< 24 meses). Pulando.', log_ctx)
        return

    logging.info('[%s] Série salarial calculada. Iniciando ajuste do modelo SARIMA (sazonal)...', log_ctx)
    try:
        if HAS_PMDARIMA:
            modelo = _auto_arima(
                serie_salarial,
                seasonal=True, m=12,
                stepwise=True, error_action='ignore', suppress_warnings=True,
            )
            previsoes_tuple = modelo.predict(
                n_periods=periodos, 
                return_conf_int=True, 
                alpha=alpha
            )
            previsoes = previsoes_tuple[0]
            conf_int = previsoes_tuple[1]
            conf_int_low = conf_int[:, 0]
            conf_int_high = conf_int[:, 1]
            
        else:
            logging.info('[%s] Usando fallback (statsmodels) para auto-SARIMA.', log_ctx)
            modelo_fit = _auto_arima_fallback(serie_salarial, seasonal=True, m=12)
            forecast_results = modelo_fit.get_forecast(steps=periodos)
            previsoes = forecast_results.predicted_mean
            conf_int = forecast_results.conf_int(alpha=alpha)
            conf_int_low = conf_int.iloc[:, 0]
            conf_int_high = conf_int.iloc[:, 1]

    except Exception as exc:
        logging.error('[%s] Falha no ajuste SARIMA: %s. Pulando.', log_ctx, exc, exc_info=True)
        return

    future_index = [serie_salarial.index[-1] + i for i in range(1, periodos + 1)]

    historico = pd.DataFrame({
        'filtro_tipo': filtro_tipo, 'filtro_valor': filtro_valor,
        'grupo_tipo': grupo_coluna_sql, 'grupo_valor': grupo_nome,
        'data': [idx.to_timestamp().date() for idx in serie_salarial.index],
        'salario_real': serie_salarial.values,
        'salario_projetado': np.nan,
        'salario_projetado_low': np.nan,
        'salario_projetado_high': np.nan,
    })
    
    previsoes_np = np.array(previsoes)
    
    futuro = pd.DataFrame({
        'filtro_tipo': filtro_tipo, 'filtro_valor': filtro_valor,
        'grupo_tipo': grupo_coluna_sql, 'grupo_valor': grupo_nome,
        'data': [idx.to_timestamp().date() for idx in future_index],
        'salario_real': np.nan,
        'salario_projetado': previsoes_np,
        'salario_projetado_low': conf_int_low,
        'salario_projetado_high': conf_int_high,
    })

    resultado = pd.concat([historico, futuro], ignore_index=True)

    with conn:
        conn.execute(
            'DELETE FROM projecoes_salariais WHERE filtro_tipo = ? AND filtro_valor = ? AND grupo_tipo = ? AND grupo_valor = ?',
            (filtro_tipo, filtro_valor, grupo_coluna_sql, grupo_nome),
        )
        resultado.to_sql('projecoes_salariais', conn, if_exists='append', index=False)
    logging.info('[%s] Projeção SARIMA (Prognóstico + Cenários) salva com sucesso.', log_ctx)


def executar_modelagem() -> None:
    """Orquestra regressão e projeções SARIMA."""
    if not DB_PATH.exists():
        raise FileNotFoundError(f'Banco {DB_PATH} não encontrado. Execute storage.py antes.')
    
    logging.info(f'Iniciando processo de modelagem. Log será salvo em: {LOG_FILE_PATH}')

    with sqlite3.connect(DB_PATH) as conn:
        
        try:
            df_reg = carregar_dados_para_regressao(conn)
            rodar_regressao(df_reg, conn)
            del df_reg # Libera memória
        except Exception as e:
            logging.error('Falha crítica ao rodar Regressão: %s', e, exc_info=True)

        logging.info('Iniciando projeções SARIMA para %d cenários...', len(CENARIOS_ARIMA))
        
        for filtro_tipo, filtro_valor in CENARIOS_ARIMA:
            for grupo_coluna_sql, grupos_codigo_lista in GRUPOS_DE_PROJECAO.items():
                
                try:
                    df_agg_grupos = carregar_dados_para_arima(conn, filtro_tipo, filtro_valor, grupo_coluna_sql, grupos_codigo_lista)
                    if df_agg_grupos.empty:
                        logging.warning(f'[{filtro_tipo}={filtro_valor}] Sem dados para grupos {grupo_coluna_sql}. Pulando.')
                        continue
                except Exception as e:
                    logging.error(f'Falha ao carregar dados SARIMA para {grupo_coluna_sql}: {e}')
                    continue
                
                for grupo_codigo in grupos_codigo_lista:
                    grupo_nome = GRUPOS_MAP[grupo_coluna_sql].get(grupo_codigo, 'NA')
                    
                    df_grupo_especifico = df_agg_grupos[df_agg_grupos['grupo_valor'] == grupo_codigo]
                    
                    try:
                        serie_salarial = calcular_serie_salarial(df_grupo_especifico)
                        executar_projecao_salarial(
                            conn, filtro_tipo, filtro_valor, 
                            grupo_coluna_sql, grupo_codigo, grupo_nome, # (correção de bug)
                            serie_salarial
                        )
                    except Exception as e:
                        logging.error(
                            'Falha crítica ao rodar SARIMA para (%s=%s, %s=%s): %s',
                            filtro_tipo, filtro_valor, grupo_coluna_sql, grupo_nome, e, exc_info=True
                        )
        
        logging.info('Processo de modelagem concluído.')


def main() -> None:
    executar_modelagem()


if __name__ == '__main__':
    main()