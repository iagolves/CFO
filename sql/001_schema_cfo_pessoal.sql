-- CFO Pessoal + Auditax — schema PostgreSQL
-- Executar uma vez em banco limpo (ou ajustar tipos/tabelas existentes).
-- Requer extensão para UUID: pgcrypto (gen_random_uuid).

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Enumerações
CREATE TYPE status_cliente AS ENUM ('Ativo', 'Inativo');

CREATE TYPE status_receita AS ENUM ('Pendente', 'Pago');

CREATE TYPE categoria_transacao AS ENUM (
  'Fixa',
  'Variável',
  'Imposto',
  'Dívida'
);

-- Parâmetros globais (CDI, câmbio, etc.)
CREATE TABLE parametros_financeiros (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  vigencia_inicio date NOT NULL,
  cdi_aa numeric(8, 4) NOT NULL CHECK (cdi_aa > 0),
  usd_brl numeric(10, 4) CHECK (usd_brl IS NULL OR usd_brl > 0),
  observacao text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_parametros_vigencia ON parametros_financeiros (vigencia_inicio DESC);

-- Clientes Auditax
CREATE TABLE clientes (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  nome text NOT NULL,
  valor_honorario numeric(14, 2) NOT NULL CHECK (valor_honorario >= 0),
  dia_vencimento smallint NOT NULL CHECK (dia_vencimento BETWEEN 1 AND 31),
  status status_cliente NOT NULL DEFAULT 'Ativo',
  honorario_vigencia_inicio date,
  pontualidade text,
  observacao text,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_clientes_nome UNIQUE (nome)
);

-- Receitas (honorários por competência)
CREATE TABLE receitas (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  cliente_id uuid NOT NULL REFERENCES clientes (id) ON DELETE RESTRICT,
  data_competencia date NOT NULL,
  data_recebimento_real date,
  status status_receita NOT NULL DEFAULT 'Pendente',
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_receitas_cliente ON receitas (cliente_id);
CREATE INDEX idx_receitas_competencia ON receitas (data_competencia);

-- Cartões de crédito
CREATE TABLE cartoes_credito (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  nome text NOT NULL,
  limite numeric(14, 2) CHECK (limite IS NULL OR limite > 0),
  melhor_dia_compra smallint CHECK (
    melhor_dia_compra IS NULL
    OR melhor_dia_compra BETWEEN 1 AND 31
  ),
  dia_vencimento smallint CHECK (
    dia_vencimento IS NULL
    OR dia_vencimento BETWEEN 1 AND 31
  ),
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_cartoes_credito_nome UNIQUE (nome)
);

-- Contas bancárias (fluxo não-cartão)
CREATE TABLE contas_bancarias (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  nome text NOT NULL,
  instituicao text,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_contas_bancarias_nome UNIQUE (nome)
);

-- Transações de caixa
CREATE TABLE transacoes (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  data date NOT NULL,
  descricao text NOT NULL,
  valor numeric(14, 2) NOT NULL,
  categoria categoria_transacao NOT NULL,
  cartao_id uuid REFERENCES cartoes_credito (id) ON DELETE SET NULL,
  conta_bancaria_id uuid REFERENCES contas_bancarias (id) ON DELETE SET NULL,
  parcela_atual smallint CHECK (parcela_atual IS NULL OR parcela_atual >= 1),
  parcela_total smallint CHECK (parcela_total IS NULL OR parcela_total >= 1),
  realizado boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ck_transacao_origem CHECK (
    (
      cartao_id IS NOT NULL
      AND conta_bancaria_id IS NULL
    )
    OR (
      cartao_id IS NULL
      AND conta_bancaria_id IS NOT NULL
    )
  ),
  CONSTRAINT ck_transacao_parcela CHECK (
    parcela_atual IS NULL
    OR parcela_total IS NULL
    OR parcela_atual <= parcela_total
  )
);

CREATE INDEX idx_transacoes_data ON transacoes (data);
CREATE INDEX idx_transacoes_cartao ON transacoes (cartao_id);
CREATE INDEX idx_transacoes_conta ON transacoes (conta_bancaria_id);

-- Dívidas e empréstimos
CREATE TABLE dividas_emprestimos (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tipo text NOT NULL,
  instituicao text NOT NULL,
  descricao text NOT NULL,
  valor_total numeric(14, 2) NOT NULL CHECK (valor_total >= 0),
  taxa_juros_mensal_pct numeric(8, 4) CHECK (
    taxa_juros_mensal_pct IS NULL
    OR taxa_juros_mensal_pct >= 0
  ),
  taxa_implicita boolean NOT NULL DEFAULT false,
  valor_parcela numeric(14, 2) NOT NULL CHECK (valor_parcela >= 0),
  parcelas_restantes smallint CHECK (
    parcelas_restantes IS NULL
    OR parcelas_restantes >= 0
  ),
  saldo_quitacao numeric(14, 2),
  prioridade smallint,
  ativo boolean NOT NULL DEFAULT true,
  termino_previsto text,
  observacoes text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_dividas_prioridade ON dividas_emprestimos (prioridade);

-- Opcional: snapshot mensal da planilha agregada (só conferência / migração)
CREATE TABLE fluxo_mensal_snapshot (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  mes_ano text NOT NULL,
  payload jsonb NOT NULL,
  fonte text NOT NULL DEFAULT 'planilha',
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_fluxo_mes_fonte UNIQUE (mes_ano, fonte)
);

-- Entradas fora do fluxo recorrente (aportes, empréstimos, resgates, reembolsos).
CREATE TABLE entradas_extras (
  id serial PRIMARY KEY,
  data text NOT NULL,
  descricao text NOT NULL,
  valor numeric(14, 2) NOT NULL,
  categoria text NOT NULL,
  origem text
);

CREATE INDEX idx_entradas_extras_data ON entradas_extras (data);

COMMENT ON TABLE parametros_financeiros IS 'CDI aa, câmbio; histórico por vigencia_inicio.';
COMMENT ON TABLE clientes IS 'Carteira Auditax; honorario_vigencia_inicio para novos contratos.';
COMMENT ON TABLE receitas IS 'Uma linha por cliente x competência (mês de referência).';
COMMENT ON TABLE transacoes IS 'Origem exclusiva: cartao_id OU conta_bancaria_id (nunca ambos, nunca nulos).';
COMMENT ON TABLE fluxo_mensal_snapshot IS 'Desnormalizado para bater com FLUXO_DE_CAIXA da planilha.';
COMMENT ON TABLE entradas_extras IS 'Aportes, empréstimos, resgates, reembolsos; categoria/origem livres.';
