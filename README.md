# Auditax CFO — Gestão Financeira Pessoal

> Dashboard financeiro pessoal construído sobre **Streamlit + Supabase (PostgreSQL)**, com visual Dark Mode premium inspirado no padrão de produto SaaS.

---

## Stack Tecnológica

| Camada | Tecnologia |
|---|---|
| Interface | [Streamlit](https://streamlit.io) >= 1.28 |
| Banco de dados | [Supabase](https://supabase.com) — PostgreSQL gerenciado |
| Driver PostgreSQL | psycopg2-binary >= 2.9 |
| Gráficos | Plotly >= 5.18 |
| Análise de dados | Pandas >= 2.0 |
| Fallback local | SQLite (desenvolvimento offline) |

---

## Funcionalidades

- **Dashboard Hero** — 4 cards: Em Caixa · A Receber · Despesas do Mês · Projeção de Saldo
- **Fluxo de Caixa Projetado** — gráfico de barras diário com saldo acumulado (inclui entradas provisionadas)
- **Entradas Extras** — lançamentos manuais com status `Realizado` ou `Provisionado`; badge visual para pendências
- **Despesas** — débito em conta e faturas de cartão com provisões futuras
- **Clientes** — carteira de honorários mensais com vigência e pontualidade
- **Dívidas** — empréstimos com taxa implícita, parcelas restantes e saldo de quitação
- **Cartões de Crédito** — controle de faturas e vencimentos
- **Realizado vs. Projetado** — variância mensal entre receitas e despesas previstas

### Fórmula do Saldo Projetado

```
Saldo Projetado = Saldo Real
                + Entradas Extras (Provisionadas)
                − Despesas Provisionadas (não realizadas)
```

---

## Estrutura do Projeto

```
.
├── app.py                  # Aplicação Streamlit (UI + lógica de apresentação)
├── database.py             # Camada de dados: SQLite local e PgConn (Supabase)
├── seed_db.py              # Popula o banco local a partir de CSVs
├── migrate_to_supabase.py  # Migração SQLite → Supabase (uso único)
├── requirements.txt
├── .gitignore
├── .streamlit/
│   └── secrets.toml        # ⚠ NÃO commitado — contém credenciais
└── import/
    └── csv/                # CSVs de seed (dados de referência)
```

---

## Configuração Local

### 1. Clonar e instalar dependências

```bash
git clone https://github.com/seu-usuario/auditax-cfo.git
cd auditax-cfo
python -m venv venv
source venv/bin/activate          # macOS/Linux
# venv\Scripts\activate           # Windows
pip install -r requirements.txt
```

### 2. Configurar credenciais Supabase

Crie o arquivo `.streamlit/secrets.toml` (nunca commitado):

```toml
[connections.postgresql]
url = "postgresql://postgres:SUA_SENHA@db.SEU_PROJETO.supabase.co:5432/postgres?sslmode=require"
```

> Sem este arquivo, o app usa SQLite local automaticamente como fallback.

### 3. Rodar localmente

```bash
streamlit run app.py
```

---

## Deploy no Streamlit Cloud

1. Faça push do repositório para o GitHub (`.streamlit/secrets.toml` está no `.gitignore`)
2. Acesse [share.streamlit.io](https://share.streamlit.io) → **New app**
3. Em **Advanced settings → Secrets**, cole o conteúdo do `secrets.toml`
4. Deploy

---

## Segurança

| Arquivo | Status |
|---|---|
| `.streamlit/secrets.toml` | `.gitignore` — credenciais nunca sobem ao repositório |
| `database.db` | `.gitignore` — banco local não versionado |
| `CFO_Pessoal_Iago.xlsx` | `.gitignore` — planilha pessoal não versionada |

---

## Licença

Uso interno — Auditax Escritório Contábil.
