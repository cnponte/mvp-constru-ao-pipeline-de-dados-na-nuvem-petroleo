# MVP: Pipeline de Dados na Nuvem — Análise da Importação de Petróleo Bruto no Brasil (2015–2025)

**Aluno:** Cássio Nascimento Ponte

**Curso:** Pós-Graduação Ciência de Dados

**Disciplina:** Engenharia de Dados

**Plataforma de Nuvem:** Databricks Free Edition (Community Cloud)  

**Repositório:** [GitHub - Pipeline de Petróleo](https://github.com/cnponte/mvp-constru-ao-pipeline-de-dados-na-nuvem-petroleo)

---

## 1. Contexto de Negócios e Perguntas

### 1.1 Contexto do Problema
O mercado de petróleo bruto no Brasil possui relevância estratégica nacional e internacional. O acompanhamento do fluxo de importação exige integrar informações regulatórias e comerciais de órgãos distintos:
- **ANP (Agência Nacional do Petróleo, Gás Natural e Biocombustíveis):** Dados consolidados de regulação nacional.
- **Comex Stat (MDIC / Governo Federal):** Dados comerciais e alfandegários discriminados por país fornecedor de origem.

Devido a diferenças históricas de formatação, unidades de medida e granularidade de registro entre as fontes públicas, construiu-se um pipeline automatizado na nuvem sob a **Arquitetura Medalhão (Bronze, Silver e Gold)** no ambiente **Databricks Lakehouse**.

### 1.2 Perguntas de Negócio
O pipeline de dados foi desenhado para responder às seguintes perguntas estratégicas:
1. **Qual foi o volume total acumulado (em m³) de petróleo bruto importado pelo Brasil entre 2015 e 2025?**
2. **Quais são os principais países de origem fornecedores de petróleo bruto para o mercado brasileiro no período analisado?**
3. **Existe convergência ou divergência relevante entre a série temporal da ANP (visão regulatória) e os registros do Comex Stat (visão alfandegária)?**
4. **Análise de Tendência de Mercado (Google Trends):** Como o interesse público de busca correlaciona-se com os volumes físicos importados ao longo do tempo?
   - O uso de séries temporais do Google Trends atua como um *proxy* de percepção pública e atenção do mercado. Essa comparação permite avaliar se momentos de oscilação de volume ou choques de oferta refletem no comportamento de busca por informações do setor energético.

### 1.3 Licença e Origem dos Dados
Os conjuntos de dados utilizados são públicos e abertos, disponibilizados pelo Governo Federal do Brasil:
- **Bases:** Dados Abertos ANP e Estatísticas de Comércio Exterior (Comex Stat/MDIC).
- **Licença de Uso:** Protegidos sob a **Lei de Acesso à Informação (Lei nº 12.527/2011)** e **Licença Aberta do Governo Federal**, permitindo livre uso, transformação e distribuição para fins acadêmicos e comerciais.

---

## 2. Carga dos Dados (Camada Bronze)

- **Storage Utilizado:** Databricks Unity Catalog Volumes (`/Volumes/workspace/default/bronze_petroleo/`).
- **Arquivos Ingeridos:**
  - `importacoes-m3-2015-2025.xlsx` (Origem: ANP)
  - `comex_importacao_petroleo_paises.xlsx` (Origem: Comex Stat)
- **Método de Carga:** Upload direto para o volume persistente na nuvem e leitura via PySpark e Pandas.

---

## 3. Modelagem e Catálogo de Dados

Foi adotado o modelo de **Arquitetura Medalhão**:
- **Bronze:** Dados brutos mantidos na estrutura original.
- **Silver:** Limpeza, remoção de nulos, unpivot de colunas anuais do Comex Stat e padronização em m³.
- **Gold:** Unificação e concatenação das bases ANP e Comex Stat para análise comparativa.

### Catálogo de Dados (Tabela Gold: `gold_importacao_petroleo`)

| Coluna | Tipo | Descrição | Domínio de Valores | Linhagem |
| :--- | :--- | :--- | :--- | :--- |
| `Data` | timestamp | Primeiro dia do mês da operação (YYYY-MM-01) | 2015-01-01 a 2025-12-01 | Calculado a partir de Ano e Mês |
| `Ano` | bigint | Ano da importação | 2015 a 2025 | Extraído do cabeçalho / coluna original |
| `Mes_Num` | bigint | Número do mês | 1 a 12 | Mapeado do nome do mês |
| `Pais_Origem` | string | País fornecedor do petróleo | Nome do país ou 'Não Informado' | Comex Stat / ANP |
| `Volume_m3` | double | Volume importado em m³ | Min: 0.0, Max: >3.000.000 | Padronização e unpivot das fontes |
| `Fonte` | string | Órgão emissor do registro | 'ANP', 'Comex Stat' | Identificador de origem no pipeline |

---

## 4. Pipeline de Dados (ETL)

O pipeline foi construído em PySpark/Pandas no Databricks, seguindo as etapas:
1. Leitura com tratamento de cabeçalhos deslocados (`skiprows`) da ANP.
2. Transformação da estrutura *wide* (matricial) do Comex Stat para formato *long* (relacional) via `melt`.
3. Padronização da unidade de medida para metros cúbicos (m³).
4. Concatenação e geração da tabela Gold unificada.

---

## 5. Qualidade de Dados

O pipeline garantiu a qualidade através de 5 dimensões:
- **Completude:** Preenchimento de valores ausentes/nulos e substituição de inconsistências por 0.
- **Consistência:** Padronização das datas no formato ISO (`YYYY-MM-01`) e uniformização da unidade de medida em m³.
- **Unicidade:** Validação e remoção de registros duplicados por combinação de `Data` + `Pais_Origem` + `Fonte`.
- **Acurácia:** Filtro de remoção de volumes negativos ou inconsistentes.
- **Outliers:** Tratamento de leitura inicial de cabeçalhos ruidosos no arquivo da ANP.

---

## 6. Análise de Dados e Resultados

### Respostas às Perguntas de Negócio:
1. **Volume Total Importado:** A análise combinada indica um fluxo contínuo de importação, com picos de volume alinhados à demanda das refinarias nacionais.
2. **Principais Países Fornecedores:** A base do Comex Stat permitiu identificar os principais países da OPEP e das Américas que lideram o fornecimento para o Brasil.
3. **Comparativo ANP vs Comex Stat:** As curvas temporais mostram forte aderência e tendência convergente entre os dados regulatórios da ANP e os dados alfandegários do Comex Stat.
4. **Tendência de Mercado:** A análise comparativa com o Google Trends demonstrou que picos pontuais na busca pelo tema coincidem com períodos de volatilidade no mercado internacional de petróleo, funcionando como um termômetro qualitativo de atenção do público em relação aos dados oficiais de importação.

---

## 7. Persistência das Tabelas na Nuvem (Delta Lake)

As tabelas finais foram gravadas no formato **Delta Lake** no catálogo do Databricks (`workspace.default.gold_importacao_petroleo`), garantindo a governança, integridade e persistência dos dados na nuvem.

*Comprovação de persistência no Catalog Explorer do Databricks:*

<img width="1907" height="897" alt="tabela gold - catalog" src="https://github.com/user-attachments/assets/e91ee5d7-9f3c-442e-aab9-d2042ccd89f6" />

---

## 8. Conclusão e Autoavaliação

### 8.1 Atingimento dos Objetivos
Os objetivos do MVP foram integralmente alcançados. Foi possível construir um pipeline funcional de ponta a ponta na nuvem, aplicando a Arquitetura Medalhão e respondendo com clareza às perguntas de negócio.

### 8.2 Dificuldades Encontradas
- **Tratamento Estrutural:** A planilha da ANP continha linhas de cabeçalho deslocadas, exigindo tratamento específico de limpeza.
- **Formato Matricial:** A base do Comex Stat apresentava os anos distribuídos em colunas, necessitando de operação de *unpivot* (`melt`) para reestruturação temporal.
- **Persistência na Nuvem:** Adequação dos tipos de dados para compatibilidade na conversão Pandas/PySpark e salvamento em formato Delta Lake.

### 8.3 Trabalhos Futuros
- Incorporar a variável de valor financeiro (US\$ FOB) para calcular o custo médio por m³ importado.
- Agendar a execução automática do pipeline utilizando o **Databricks Workflows**.
