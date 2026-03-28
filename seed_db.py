#!/usr/bin/env python3
"""
Popula database.db (SQLite) a partir dos CSVs.

Ordem: import/csv/_ordem_carga.txt
Clientes: somente clientes_completo.csv (20 clientes Auditax).
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import database as db


def _resolve_csv_dir(base: Path) -> Path:
    """Aceita pasta `csv` na raiz ou `import/csv` (layout atual do projeto)."""
    for candidate in (base / "import" / "csv", base / "csv"):
        if candidate.is_dir() and (candidate / "_ordem_carga.txt").exists():
            return candidate
    for candidate in (base / "import" / "csv", base / "csv"):
        if candidate.is_dir():
            return candidate
    raise FileNotFoundError(
        f"Nenhuma pasta de CSV encontrada em {base}/import/csv ou {base}/csv"
    )


def _count(conn: sqlite3.Connection, table: str) -> int:
    row = conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()
    return int(row[0])


def main() -> int:
    base = Path(__file__).resolve().parent
    db_path = base / "database.db"
    csv_dir = _resolve_csv_dir(base)
    clientes_csv = csv_dir / "clientes_completo.csv"

    print(f"Banco: {db_path}")
    print(f"CSV:   {csv_dir}")
    print(f"Schema SQLite (ids inteiros AUTOINCREMENT): v{db.SCHEMA_VERSION}")
    print()

    if not clientes_csv.is_file():
        print(f"ERRO: arquivo obrigatório ausente: {clientes_csv}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        db.init_schema(conn)
        db._clear_data_tables(conn)

        db.load_parametros(conn, csv_dir / "parametros_financeiros.csv")
        conn.commit()
        print(f"[OK] parametros_financeiros — {_count(conn, 'parametros_financeiros')} registro(s)")

        map_cartoes: dict[str, int] = {}
        db.load_cartoes(conn, csv_dir / "cartoes_credito.csv", map_cartoes)
        conn.commit()
        print(f"[OK] cartoes_credito — {_count(conn, 'cartoes_credito')} registro(s)")

        map_contas: dict[str, int] = {}
        db.load_contas(conn, csv_dir / "contas_bancarias.csv", map_contas)
        conn.commit()
        print(f"[OK] contas_bancarias — {_count(conn, 'contas_bancarias')} registro(s)")

        map_clientes: dict[str, int] = {}
        db.load_clientes_completo(conn, clientes_csv, map_clientes)
        conn.commit()
        print(f"[OK] clientes (clientes_completo.csv) — {_count(conn, 'clientes')} registro(s)")

        db.load_dividas(conn, csv_dir / "dividas_emprestimos.csv")
        conn.commit()
        print(f"[OK] dividas_emprestimos — {_count(conn, 'dividas_emprestimos')} registro(s)")

        db.load_receitas(conn, csv_dir / "receitas.csv", map_clientes)
        conn.commit()
        print(f"[OK] receitas — {_count(conn, 'receitas')} registro(s)")

        db.load_transacoes(conn, csv_dir / "transacoes.csv", map_cartoes, map_contas)
        conn.commit()
        print(f"[OK] transacoes — {_count(conn, 'transacoes')} registro(s)")

        print()
        print("Carga concluída.")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
