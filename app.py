import sqlite3
from datetime import date
from pathlib import Path
from typing import Tuple, List, Dict

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import logging

# --- CONFIGURAÇÕES E DICIONÁRIOS ---
DB_PATH = (Path(__file__).resolve().parent / 'projeto_caged.db')

# Paleta de cores (ajuste conforme sua identidade visual)
PRIMARY = '#1F4AA8'
PALETTE = ['#1F4AA8', '#4E79A7', '#F28E2B', '#E15759', '#76B7B2', '#59A14F', '#EDC948', '#B07AA1', '#9C755F', '#BAB0AC']
px.defaults.color_discrete_sequence = PALETTE

# Mapas para "traduzir" códigos em nomes amigáveis (usados nos gráficos)
# Mantemos apenas o contexto geral para simplificar a aplicação
PROJECAO_PADRAO = ('geral', 'brasil')
GENERO_MAP = {'1': 'Homem', '3': 'Mulher'} # Layout CAGED: 1=Masc, 3=Fem
RACA_MAP = {
    '1': 'Branca', '2': 'Preta', '3': 'Parda',
    '4': 'Amarela', '5': 'Indígena', '6': 'Não informada',
    '9': 'Não Identificado',
}

# --- FUNÇÕES DE DADOS (COM CACHE CORRIGIDO) ---

def _db_cache_version() -> float:
    """Versão de cache baseada no mtime do arquivo do banco.
    Ao alterar o DB, esse valor muda e invalida o cache do Streamlit.
    """
    try:
        return DB_PATH.stat().st_mtime
    except Exception:
        return 0.0

@st.cache_data(ttl=3600)
def get_data_bounds(_v: float) -> Tuple[str, str]:
    """Busca a data mínima e máxima dos dados para o slider."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = pd.read_sql('SELECT MIN(data) AS min_d, MAX(data) AS max_d FROM dados_agregados', conn).iloc[0]
            return (row['min_d'], row['max_d'])
    except Exception:
        return ('2020-01', '2023-12') # Fallback

# (Removido) A antiga projeção de "hiato" não é mais usada no layout simplificado.

@st.cache_data(ttl=3600)
def carregar_projecoes_grupo(
    filtro_tipo: str,
    filtro_valor: str,
    grupo_tipo: str,
    grupos_valores: List[str],
    _v: float,
) -> pd.DataFrame:
    """Carrega séries reais e projetadas por grupo (tabela projecoes_salariais)."""
    if not grupos_valores:
        return pd.DataFrame()
    placeholders = ','.join(['?'] * len(grupos_valores))
    query = f"""
        SELECT filtro_tipo, filtro_valor, grupo_tipo, grupo_valor, data,
               salario_real, salario_projetado, salario_projetado_low, salario_projetado_high
        FROM projecoes_salariais
        WHERE filtro_tipo = ? AND filtro_valor = ?
          AND grupo_tipo = ? AND grupo_valor IN ({placeholders})
        ORDER BY data
    """
    params = [filtro_tipo, filtro_valor, grupo_tipo] + grupos_valores
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql(query, conn, params=params)
    if df.empty:
        return df
    df['data'] = pd.to_datetime(df['data'])
    return df

@st.cache_data(ttl=3600)
def carregar_regressao(_v: float) -> pd.DataFrame:
    """Carrega os coeficientes da regressão."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            df = pd.read_sql('SELECT * FROM coeficientes_regressao', conn)
        df['coeficiente'] = (df['coeficiente'] * 100).round(2)
        df['p_valor'] = df['p_valor'].round(4)
        df = df.set_index('variavel')
        return df
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def carregar_dados_historicos(data_ini: str, data_fim: str, _v: float) -> pd.DataFrame:
    """Carrega SOMAS e calcula médias (contexto geral, sem filtros de município/CNAE)."""
    sql = f'''
        SELECT 
            data, genero, raca_cor, grau_instrucao, "tipodedeficiência",
            SUM(total_admissoes) AS total_admissoes,
            SUM(soma_salario) AS soma_salario,
            SUM(soma_idade) AS soma_idade
        FROM dados_agregados
        WHERE data >= ? AND data <= ?
        GROUP BY data, genero, raca_cor, grau_instrucao, "tipodedeficiência"
        ORDER BY data
    '''
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql(sql, conn, params=[data_ini, data_fim])

    df_validos = df[df['total_admissoes'] > 0].copy()
    if df_validos.empty:
        return pd.DataFrame()

    df_validos['salario_medio_ponderado'] = df_validos['soma_salario'] / df_validos['total_admissoes']
    df_validos['idade_media_ponderada'] = df_validos['soma_idade'] / df_validos['total_admissoes']
    df_validos['genero_nome'] = df_validos['genero'].astype(str).map(GENERO_MAP).fillna('Outro')
    df_validos['raca_nome'] = df_validos['raca_cor'].astype(str).map(RACA_MAP).fillna('Outro')
    df_validos['data'] = pd.to_datetime(df_validos['data'])
    return df_validos

# --- LAYOUT PRINCIPAL DO DASHBOARD ---

st.set_page_config(layout='wide', page_title='Análise Salarial - CAGED')
st.title('Análise Salarial - CAGED')

# --- SIDEBAR (Filtros APENAS para Análise Histórica) ---
st.sidebar.header('Período')

try:
    min_d_str, max_d_str = get_data_bounds(_db_cache_version())
    min_date = date.fromisoformat(min_d_str + '-01')
    max_date = date.fromisoformat(max_d_str + '-01')
    filtro_data = st.sidebar.slider(
        'Período Histórico',
        min_value=min_date, max_value=max_date,
        value=(min_date, max_date), format='YYYY-MM'
    )
    data_ini, data_fim = [d.strftime('%Y-%m') for d in filtro_data]
except Exception as e:
    st.sidebar.error(f'Banco de dados não encontrado ou vazio. {e}')
    st.stop()

# --- CARREGAMENTO DE DADOS HISTÓRICOS (Baseado na Sidebar) ---
df_historico = carregar_dados_historicos(data_ini, data_fim, _db_cache_version())
df_fatores = carregar_regressao(_db_cache_version())

# --- CORPO PRINCIPAL (KPIs e TABS) ---
st.header('Contexto geral (Brasil)')
st.markdown(f"Período: **{data_ini}** a **{data_fim}**")

if df_historico.empty:
    st.warning('Nenhum dado histórico encontrado para os filtros selecionados.')
    st.stop()

# (CORRIGIDO v6) Cálculo de KPIs com base nas SOMAS
kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
total_admissoes = int(df_historico['total_admissoes'].sum())
kpi_col1.metric("Total de Admissões", f"{total_admissoes:,}".replace(',', '.'))

try:
    df_genero_kpi = df_historico.groupby('genero_nome').agg(
        soma_salario_total=('soma_salario', 'sum'),
        total_admissoes_total=('total_admissoes', 'sum')
    )
    
    salario_homens_medio = df_genero_kpi.loc['Homem', 'soma_salario_total'] / df_genero_kpi.loc['Homem', 'total_admissoes_total']
    salario_mulheres_medio = df_genero_kpi.loc['Mulher', 'soma_salario_total'] / df_genero_kpi.loc['Mulher', 'total_admissoes_total']
    
    hiato_atual = (salario_mulheres_medio / salario_homens_medio) * 100
    kpi_col2.metric("Hiato Salarial (M/H)", f"{hiato_atual:.1f}%", f"{salario_mulheres_medio:,.2f} / {salario_homens_medio:,.2f}")

except Exception as e:
    logging.warning(f"Erro ao calcular KPI de hiato: {e}")
    kpi_col2.metric("Hiato Salarial (M/H)", "N/D")

soma_salario_geral = df_historico['soma_salario'].sum()
salario_medio_geral = soma_salario_geral / total_admissoes
kpi_col3.metric("Salário Médio de Admissão", f"R$ {salario_medio_geral:,.2f}")

st.divider()

# --- TABS DE VISUALIZAÇÃO ---
tab_projecao, tab_historico, tab_fatores = st.tabs([
    "Projeções (SARIMA)", 
    "Análise Histórica", 
    "Fatores (Regressão)"
])

# --- TAB 1: PROJEÇÃO (LÓGICA SEPARADA) ---
with tab_projecao:
    st.subheader('Projeções por Grupo (SARIMA)')
    filtro_tipo, filtro_valor = PROJECAO_PADRAO
    # Seleção de grupo
    grupo_nomes = {
        'Gênero': ('genero', {'MASC': 'Homens', 'FEM': 'Mulheres'}),
        'Raça/Cor': ('raca_cor', {'BRANCA': 'Branca', 'PRETA': 'Preta', 'PARDA': 'Parda'}),
        'Escolaridade': ('grau_instrucao', {
            '1':'Analfabeto','2':'Fund. Incompl.','3':'Fund. Compl.','4':'Médio Incompl.','5':'Médio Compl.','6':'Sup. Incompl.','7':'Sup. Compl.','8':'Pós-grad.','9':'Mestrado','10':'Doutorado','11':'Pós-doc.'
        }),
        'Deficiência': ('tipodedeficiência', {
            '0':'Não deficiente','1':'Física','2':'Auditiva','3':'Visual','4':'Intelectual','5':'Múltipla','6':'Reabilitado','9':'Não Identificado'
        }),
    }
    grupo_tipo_nome = st.selectbox('Grupo', options=list(grupo_nomes.keys()))
    grupo_tipo_sql, legenda = grupo_nomes[grupo_tipo_nome]
    opcoes = list(legenda.keys())
    cols = st.columns(2)
    # Série A sempre disponível
    escolha1 = cols[0].selectbox('Série A', options=opcoes, index=0)
    # Série B: proteger contra lista vazia ao remover escolha1
    escolha2 = None
    if len(opcoes) >= 2:
        opcoes_b = [o for o in opcoes if o != escolha1]
        # Se por algum motivo a lista ficar vazia, recorre à lista original
        if not opcoes_b:
            opcoes_b = opcoes
        escolha2 = cols[1].selectbox('Série B', options=opcoes_b, index=0)
    else:
        cols[1].info('Só há uma categoria disponível para comparação.')

    selecoes = [c for c in [escolha1, escolha2] if c]
    df_grupos = carregar_projecoes_grupo(filtro_tipo, filtro_valor, grupo_tipo_sql, selecoes, _db_cache_version())
    if df_grupos.empty:
        st.info('Sem projeções salvas para este grupo. Execute a modelagem para gerar projecoes_salariais.')
    else:
        fig_cmp = go.Figure()
        for cod in selecoes:
            sub = df_grupos[df_grupos['grupo_valor'] == cod]
            fig_cmp.add_trace(go.Scatter(x=sub['data'], y=sub['salario_real'], mode='lines+markers', name=f"{legenda.get(cod, cod)} (Real)", line=dict(color=PALETTE[0])))
            sp = sub.dropna(subset=['salario_projetado'])
            if not sp.empty:
                fig_cmp.add_trace(go.Scatter(x=sp['data'], y=sp['salario_projetado_low'], mode='lines', line=dict(width=0), showlegend=False))
                fig_cmp.add_trace(go.Scatter(x=sp['data'], y=sp['salario_projetado_high'], mode='lines', fill='tonexty', line=dict(width=0), name=f"{legenda.get(cod, cod)} (Intervalo)"))
                fig_cmp.add_trace(go.Scatter(x=sp['data'], y=sp['salario_projetado'], mode='lines', line=dict(dash='dash'), name=f"{legenda.get(cod, cod)} (Proj.)"))
        fig_cmp.update_layout(title=f'Comparação de Séries ({grupo_tipo_nome})', yaxis_title='Salário Médio (R$)', xaxis_title='Data')
        st.plotly_chart(fig_cmp, use_container_width=True)

# --- TAB 2: ANÁLISE HISTÓRICA (usa filtros da sidebar) ---
with tab_historico:
    st.subheader('Evolução das Admissões')
    col1, col2 = st.columns(2)
    # Gênero
    df_genero_hist = df_historico.groupby(['data', 'genero_nome'])['total_admissoes'].sum().reset_index()
    fig_genero = px.bar(df_genero_hist, x='data', y='total_admissoes', color='genero_nome', barmode='group', labels={'total_admissoes':'Admissões','data':'Mês','genero_nome':'Gênero'})
    col1.plotly_chart(fig_genero, use_container_width=True)
    # Raça
    df_raca_hist = df_historico.groupby(['data', 'raca_nome'])['total_admissoes'].sum().reset_index()
    fig_raca = px.bar(df_raca_hist, x='data', y='total_admissoes', color='raca_nome', barmode='stack', labels={'total_admissoes':'Admissões','data':'Mês','raca_nome':'Raça/Cor'})
    col2.plotly_chart(fig_raca, use_container_width=True)
    # Escolaridade
    df_esc_hist = df_historico.groupby(['data', 'grau_instrucao'])['total_admissoes'].sum().reset_index()
    fig_esc = px.bar(df_esc_hist, x='data', y='total_admissoes', color='grau_instrucao', barmode='stack', labels={'total_admissoes':'Admissões','data':'Mês','grau_instrucao':'Escolaridade'})
    st.plotly_chart(fig_esc, use_container_width=True)
    # Deficiência (se existir)
    if 'tipodedeficiência' in df_historico.columns:
        df_def_hist = df_historico.groupby(['data', 'tipodedeficiência'])['total_admissoes'].sum().reset_index()
        fig_def = px.bar(df_def_hist, x='data', y='total_admissoes', color='tipodedeficiência', barmode='stack', labels={'total_admissoes':'Admissões','data':'Mês','tipodedeficiência':'Deficiência'})
        st.plotly_chart(fig_def, use_container_width=True)

# --- TAB 3: FATORES DE IMPACTO (Regressão) ---
with tab_fatores:
    st.subheader('Fatores de Impacto no Salário (Regressão)')
    if df_fatores.empty:
        st.info('Nenhum resultado de regressão disponível. (Execute modelagem.py)')
    else:
        # Helper para construir gráfico por categoria a partir da tabela de coeficientes
        def grafico_por_categoria(prefixo: str, categorias: Dict[str, str], titulo: str):
            rows = []
            # Definimos como base a menor chave por ordem natural; as demais buscam dummy "prefixo_codigo"
            base = sorted(categorias.keys(), key=lambda x: (len(x), x))[0]
            for cod, nome in categorias.items():
                if cod == base:
                    coef = 0.0
                    pval = None
                else:
                    var = f'{prefixo}_{cod}'
                    coef = float(df_fatores.loc[var, 'coeficiente']) if var in df_fatores.index else 0.0
                    pval = float(df_fatores.loc[var, 'p_valor']) if var in df_fatores.index else None
                rows.append({'categoria': nome, 'impacto_pct': round(coef, 2), 'p_valor': pval})
            dfx = pd.DataFrame(rows).sort_values('impacto_pct')
            fig = px.bar(dfx, x='impacto_pct', y='categoria', orientation='h',
                         color=(dfx['p_valor'].fillna(1.0) < 0.05),
                         color_discrete_map={True: PRIMARY, False: '#BBBBBB'},
                         labels={'impacto_pct': 'Impacto (%)', 'categoria': ''},
                        )
            fig.update_layout(title=titulo)
            st.plotly_chart(fig, use_container_width=True)

        colA, colB = st.columns(2)
        with colA:
            grafico_por_categoria('genero_nome', {'MASC':'Homens','FEM':'Mulheres'}, 'Gênero (base: Homens)')
        with colB:
            grafico_por_categoria('raca_cor', {
                '1':'Branca','2':'Preta','3':'Parda','4':'Amarela','5':'Indígena','6':'Não informada','9':'Não Identificado'
            }, 'Raça/Cor (base: Branca)')

        st.markdown('---')
        colC, colD = st.columns(2)
        with colC:
            grafico_por_categoria('grau_instrucao', {
                '1':'Analfabeto','2':'Fund. Incompl.','3':'Fund. Compl.','4':'Médio Incompl.','5':'Médio Compl.',
                '6':'Sup. Incompl.','7':'Sup. Compl.','8':'Pós-grad.','9':'Mestrado','10':'Doutorado','11':'Pós-doc.'
            }, 'Escolaridade (base: Analfabeto)')
        with colD:
            grafico_por_categoria('tipo_def', {
                '0':'Não deficiente','1':'Física','2':'Auditiva','3':'Visual','4':'Intelectual','5':'Múltipla','6':'Reabilitado','9':'Não Identificado'
            }, 'Deficiência (base: Não deficiente)')