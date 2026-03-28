-- Tabela: dividas_estrategicas
-- Execute este script no SQL Editor do Supabase (supabase.com → SQL Editor)

CREATE TABLE IF NOT EXISTS dividas_estrategicas (
    id                   SERIAL PRIMARY KEY,
    nome                 TEXT        NOT NULL,
    instituicao          TEXT,
    valor_quitacao_alvo  NUMERIC(12,2) NOT NULL DEFAULT 0,
    parcela_mensal       NUMERIC(12,2) NOT NULL DEFAULT 0,
    status               TEXT        NOT NULL DEFAULT 'Pendente'
                             CHECK (status IN ('Pendente', 'QUITADA')),
    regra_ouro           BOOLEAN     NOT NULL DEFAULT FALSE,
    ordem_pagamento      INTEGER,
    observacao           TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Trigger para atualizar updated_at automaticamente
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_dividas_estrategicas_updated_at ON dividas_estrategicas;
CREATE TRIGGER trg_dividas_estrategicas_updated_at
    BEFORE UPDATE ON dividas_estrategicas
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Exemplo de dados (adapte à sua realidade):
-- INSERT INTO dividas_estrategicas (nome, instituicao, valor_quitacao_alvo, parcela_mensal, status, regra_ouro, ordem_pagamento, observacao)
-- VALUES
--   ('Crédito Pessoal',    'Mercado Pago', 3200.00, 480.00, 'Pendente', false, 1, 'Quitar primeiro — maior taxa'),
--   ('Financiamento Carro','Itaú',         18000.00, 950.00, 'Pendente', true,  2, 'Taxa baixa — manter parcelas'),
--   ('Cartão Rotativo',    'BRB',          1500.00,  300.00, 'Pendente', false, 3, NULL);
