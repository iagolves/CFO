"""
migrate_to_supabase.py
──────────────────────
Migra dados do SQLite local (database.db) → Supabase (PostgreSQL).

Uso:
    python migrate_to_supabase.py

Comportamento:
  • Preserva os IDs originais do SQLite para não quebrar FKs.
  • Usa ON CONFLICT DO NOTHING → idempotente (pode rodar várias vezes).
  • Ao final, corrige as sequences SERIAL para o próximo id livre.
  • Imprime progresso por tabela.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import psycopg2
import psycopg2.extras
import toml

# ── Caminhos ──────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
SQLITE_PATH = BASE_DIR / "database.db"
SECRETS_PATH = BASE_DIR / ".streamlit" / "secrets.toml"

# ── Ordem de migração (respeita dependências de FK) ───────────────────────────
# Tabelas sem dados locais são incluídas para reset de sequence.
TABLE_ORDER = [
    "parametros_financeiros",
    "contas_bancarias",
    "cartoes_credito",
    "clientes",
    "receitas",
    "dividas_emprestimos",
    "fluxo_mensal_snapshot",
    "transacoes",
    "entradas_extras",
    "faturas_pagas",
    "pagamentos_dividas",
    "despesas_provisionadas",
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def _pg_url() -> str:
    data = toml.load(str(SECRETS_PATH))
    url = data["connections"]["postgresql"]["url"]
    if not url:
        raise RuntimeError("URL do Supabase não encontrada em secrets.toml")
    return url


def _sqlite_cols(sq_conn: sqlite3.Connection, table: str) -> list[str]:
    cur = sq_conn.execute(f"SELECT * FROM {table} LIMIT 0")
    return [d[0] for d in cur.description] if cur.description else []


def _migrate_table(
    sq_conn: sqlite3.Connection,
    pg_cur: psycopg2.extensions.cursor,
    table: str,
) -> int:
    """
    Lê todos os registros da tabela no SQLite e insere no PostgreSQL.
    Retorna o número de linhas inseridas (ou ignoradas por conflito).
    """
    cols = _sqlite_cols(sq_conn, table)
    if not cols:
        print(f"  ⚠  {table}: sem colunas detectadas — pulando.")
        return 0

    rows = sq_conn.execute(f"SELECT * FROM {table}").fetchall()
    if not rows:
        print(f"  ·  {table}: 0 registros — nada a migrar.")
        return 0

    col_list = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    sql = (
        f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
        f"ON CONFLICT DO NOTHING"
    )

    inserted = 0
    for row in rows:
        values = tuple(row)
        try:
            pg_cur.execute(sql, values)
            if pg_cur.rowcount > 0:
                inserted += 1
        except Exception as exc:
            print(f"  ✗  {table} | linha id={row[0]} → ERRO: {exc}")
            # Rollback apenas desta linha; continua as demais
            pg_cur.connection.rollback()

    return inserted


def _reset_sequence(pg_cur, table: str) -> None:
    """Avança a SERIAL sequence para max(id)+1 para evitar conflito futuro."""
    pg_cur.execute(f"SELECT MAX(id) FROM {table}")
    row = pg_cur.fetchone()
    max_id = row[0] if row and row[0] is not None else 0
    if max_id > 0:
        pg_cur.execute(
            f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), %s)",
            (max_id,),
        )


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    if not SQLITE_PATH.exists():
        print(f"✗ database.db não encontrado em: {SQLITE_PATH}")
        sys.exit(1)
    if not SECRETS_PATH.exists():
        print(f"✗ secrets.toml não encontrado em: {SECRETS_PATH}")
        sys.exit(1)

    print("=" * 60)
    print("  Migração SQLite → Supabase")
    print("=" * 60)

    # ── Conectar SQLite ───────────────────────────────────────────
    sq_conn = sqlite3.connect(str(SQLITE_PATH))
    sq_conn.row_factory = sqlite3.Row
    print(f"\n✔ SQLite conectado: {SQLITE_PATH.name}")

    # ── Conectar Supabase ─────────────────────────────────────────
    pg_url = _pg_url()
    pg_conn = psycopg2.connect(pg_url)
    pg_conn.autocommit = False
    pg_cur = pg_conn.cursor()
    print("✔ Supabase conectado\n")

    total_inserted = 0

    for table in TABLE_ORDER:
        print(f"→ Migrando: {table}")
        try:
            n = _migrate_table(sq_conn, pg_cur, table)
            pg_conn.commit()
            _reset_sequence(pg_cur, table)
            pg_conn.commit()
            print(f"  ✔ {table}: {n} registro(s) inserido(s).")
            total_inserted += n
        except Exception as exc:
            pg_conn.rollback()
            print(f"  ✗ {table}: falha geral → {exc}")

    sq_conn.close()
    pg_cur.close()
    pg_conn.close()

    print()
    print("=" * 60)
    print(f"  Migração concluída. Total inserido: {total_inserted} registros.")
    print("=" * 60)


if __name__ == "__main__":
    main()
