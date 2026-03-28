-- Dados de exemplo (mesmos UUIDs dos CSV em import/csv/).
-- Útil para testar o schema sem COPY. Rode após 001_schema_cfo_pessoal.sql

INSERT INTO parametros_financeiros (id, vigencia_inicio, cdi_aa, usd_brl, observacao)
VALUES (
  'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaa0001',
  '2026-03-01',
  10.75,
  5.32,
  'Referência auditoria 13/03/2026'
);

INSERT INTO cartoes_credito (id, nome) VALUES
  ('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbb001', 'Cartão MP'),
  ('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbb002', 'Cartão Bco Inter'),
  ('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbb003', 'Cartão Itaú Black'),
  ('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbb004', 'Cartão Nação BRB'),
  ('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbb005', 'Cartão Itaú Azul');

INSERT INTO contas_bancarias (id, nome, instituicao)
VALUES (
  'cccccccc-cccc-4ccc-8ccc-cccccccc0001',
  'Conta corrente — operacional',
  'Caixa operacional (ex.: Itaú / BRB)'
);

INSERT INTO clientes (
  id, nome, valor_honorario, dia_vencimento, status, pontualidade
)
VALUES (
  '11111111-1111-4111-8111-000000000001',
  'Instituto Esportivo Crescer',
  350.00,
  1,
  'Ativo',
  'Pontual'
);

INSERT INTO dividas_emprestimos (
  id, tipo, instituicao, descricao, valor_total,
  taxa_juros_mensal_pct, taxa_implicita, valor_parcela,
  parcelas_restantes, saldo_quitacao, prioridade, ativo,
  termino_previsto, observacoes
)
VALUES (
  '99999999-9999-4999-8999-999999990001',
  'Empréstimo',
  'Itaú Azul',
  'Empréstimo Itaú Azul',
  5346.20,
  6.80,
  false,
  1336.55,
  4,
  3785.59,
  2,
  true,
  '10/2026',
  'Exemplo — linha 2 AUDITORIA_DETALHADA'
);

INSERT INTO receitas (
  id, cliente_id, data_competencia, data_recebimento_real, status
)
VALUES (
  'eeeeeeee-eeee-4eee-8eee-eeeeeeee0001',
  '11111111-1111-4111-8111-000000000001',
  '2026-03-01',
  '2026-03-01',
  'Pago'
);

INSERT INTO transacoes (
  id, data, descricao, valor, categoria,
  cartao_id, conta_bancaria_id, realizado
)
VALUES (
  'ffffffff-ffff-4fff-8fff-ffffffff0001',
  '2026-03-19',
  'Taxa JUCEG',
  -352.00,
  'Imposto',
  NULL,
  'cccccccc-cccc-4ccc-8ccc-cccccccc0001',
  true
);
