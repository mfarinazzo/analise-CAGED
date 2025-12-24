# Análise de Diversidade e Igualdade no Mercado de Trabalho Brasileiro (Novo CAGED)

**Autores:** Murilo Farinazzo Vieira e Nikolas Ribeiro Klasa

Este projeto tem como objetivo analisar a diversidade e a igualdade no mercado de trabalho brasileiro utilizando dados públicos do Novo CAGED (Cadastro Geral de Empregados e Desempregados). O estudo abrange desde a coleta de dados até a criação de modelos preditivos e um dashboard interativo.

## Resumo do Projeto

O projeto foi estruturado em quatro fases principais de execução:

### 1. Coleta e Pré-processamento de Dados
- **Fonte de Dados:** Base de dados pública do Novo CAGED (Ministério do Trabalho e Emprego).
- **Processamento:** Limpeza de atributos inválidos e tratamento de outliers para garantir a qualidade dos dados.

### 2. Análise Exploratória de Dados (EDA)
- **Foco:** Quatro pilares de diversidade: Gênero, Raça, Escolaridade e Deficiências.
- **Visualização:** Gráficos de colunas, linhas e box-plots para entender a distribuição dos dados.

### 3. Modelagem Supervisionada (Regressão OLS)
- **Objetivo:** Quantificar o impacto de fatores de diversidade na remuneração.
- **Análises:**
    - **Gênero:** Identificação de hiatos salariais.
    - **Raça:** Comparação da desvalorização salarial de grupos minoritários.
    - **Escolaridade:** Correlação entre níveis de ensino e aumento salarial.
    - **Deficiência:** Comparativo salarial entre pessoas com e sem deficiência.

### 4. Predição e Séries Temporais (SARIMA)
- **Modelo:** SARIMA (Seasonal AutoRegressive Integrated Moving Average).
- **Aplicação:** Projeção de tendências futuras e cenários de paridade salarial com sazonalidade ajustada (m=12).

### 5. Apresentação dos Resultados
- **Produto Final:** Dashboard Interativo desenvolvido em Streamlit para visualização das métricas e predições.

---

## Tecnologias Utilizadas

- **Linguagem:** Python
- **Framework de Dashboard:** Streamlit
- **Banco de Dados:** SQLite
- **Bibliotecas Principais:**
    - `pandas` (Manipulação de dados)
    - `plotly` (Visualização de dados)
    - `statsmodels` & `pmdarima` (Modelagem estatística e séries temporais)
    - `ftplib` (Coleta de dados via FTP)

## Como Executar o Projeto

### Pré-requisitos

Certifique-se de ter o Python instalado. Instale as dependências necessárias (exemplo baseado nas importações):

```bash
pip install pandas plotly streamlit statsmodels pmdarima
```

### Passo a Passo

1.  **Coleta de Dados:**
    Execute o script para baixar os microdados do servidor FTP do CAGED.
    ```bash
    python buscaCaged.py
    ```

2.  **Processamento de Dados:**
    Processe os arquivos baixados e popule o banco de dados (a ordem pode variar dependendo do fluxo exato, mas geralmente envolve conversão e agregação).
    ```bash
    python converterCSV.py
    python processador_agregado.py
    ```

3.  **Modelagem:**
    Execute a modelagem estatística (OLS e SARIMA).
    ```bash
    python modelagem.py
    ```

4.  **Dashboard:**
    Inicie a aplicação Streamlit para visualizar os resultados.
    ```bash
    streamlit run app.py
    ```

## Estrutura de Arquivos

- `app.py`: Aplicação principal do Dashboard (Streamlit).
- `buscaCaged.py`: Script para download automático dos dados do FTP do CAGED.
- `converterCSV.py`: Utilitário para conversão e tratamento inicial dos dados.
- `processador_agregado.py`: Script para agregação e preparação dos dados para análise.
- `modelagem.py`: Implementação dos modelos de regressão OLS e séries temporais SARIMA.
- `storage.py`: Gerenciamento de armazenamento/banco de dados.
- `graficos/`: Módulo contendo a lógica de geração de gráficos para cada pilar de diversidade.
- `viz/`: Recursos visuais estáticos (HTML/JSON).
