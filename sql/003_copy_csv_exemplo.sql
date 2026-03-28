-- Carga via COPY. Execute no diretório raiz do projeto (pasta que contém import/csv):
--   psql $DATABASE_URL -f sql/003_copy_csv_exemplo.sql
-- String vazia vira NULL (obrigatório para cartao_id nas transações).

\copy parametros_financeiros (id, vigencia_inicio, cdi_aa, usd_brl, observacao) FROM 'import/csv/parametros_financeiros.csv' WITH (FORMAT csv, HEADER, NULL '');

\copy cartoes_credito (id, nome, limite, melhor_dia_compra, dia_vencimento) FROM 'import/csv/cartoes_credito.csv' WITH (FORMAT csv, HEADER, NULL '');

\copy contas_bancarias (id, nome, instituicao) FROM 'import/csv/contas_bancarias.csv' WITH (FORMAT csv, HEADER, NULL '');

\copy clientes (id, nome, valor_honorario, dia_vencimento, status, honorario_vigencia_inicio, pontualidade, observacao) FROM 'import/csv/clientes_completo.csv' WITH (FORMAT csv, HEADER, NULL '');

\copy dividas_emprestimos (id, tipo, instituicao, descricao, valor_total, taxa_juros_mensal_pct, taxa_implicita, valor_parcela, parcelas_restantes, saldo_quitacao, prioridade, ativo, termino_previsto, observacoes) FROM 'import/csv/dividas_emprestimos.csv' WITH (FORMAT csv, HEADER, NULL '');

\copy receitas (id, cliente_id, data_competencia, data_recebimento_real, status) FROM 'import/csv/receitas.csv' WITH (FORMAT csv, HEADER, NULL '');

\copy transacoes (id, data, descricao, valor, categoria, cartao_id, conta_bancaria_id, parcela_atual, parcela_total, realizado) FROM 'import/csv/transacoes.csv' WITH (FORMAT csv, HEADER, NULL '');
