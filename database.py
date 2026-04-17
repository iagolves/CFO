"""
SQLite local — espelho lógico do schema em sql/001_schema_cfo_pessoal.sql (PostgreSQL).
Carga CSV seguindo import/csv/_ordem_carga.txt; clientes apenas via clientes_completo.csv.
"""

from __future__ import annotations

import calendar
import csv
import re as _re
import sqlite3
from datetime import date
from pathlib import Path

try:
    import psycopg2 as _psycopg2
    import psycopg2.extras as _psycopg2_extras
    _PG_AVAILABLE = True
except ImportError:  # pragma: no cover
    _PG_AVAILABLE = False

BASE_DIR = Path(__file__).resolve().parent
CSV_DIR = BASE_DIR / "import" / "csv"
# Sempre caminho absoluto explícito (Streamlit / OneDrive / cwd variam).
DB_PATH: Path = (BASE_DIR / "database.db").resolve()

CLIENTES_CSV = CSV_DIR / "clientes_completo.csv"

# Incrementar ao mudar o schema (recria tabelas na próxima abertura se v < SCHEMA_VERSION).
SCHEMA_VERSION = 4

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS parametros_financeiros (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  vigencia_inicio TEXT NOT NULL,
  cdi_aa REAL NOT NULL CHECK (cdi_aa > 0),
  usd_brl REAL CHECK (usd_brl IS NULL OR usd_brl > 0),
  observacao TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_parametros_vigencia ON parametros_financeiros (vigencia_inicio DESC);

CREATE TABLE IF NOT EXISTS clientes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  nome TEXT NOT NULL,
  valor_honorario REAL NOT NULL CHECK (valor_honorario >= 0),
  dia_vencimento INTEGER NOT NULL CHECK (dia_vencimento BETWEEN 1 AND 31),
  status TEXT NOT NULL DEFAULT 'Ativo' CHECK (status IN ('Ativo', 'Inativo')),
  honorario_vigencia_inicio TEXT,
  pontualidade TEXT,
  observacao TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  CONSTRAINT uq_clientes_nome UNIQUE (nome)
);

CREATE TABLE IF NOT EXISTS receitas (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  cliente_id INTEGER NOT NULL REFERENCES clientes (id) ON DELETE RESTRICT,
  data_competencia TEXT NOT NULL,
  data_recebimento_real TEXT,
  status TEXT NOT NULL DEFAULT 'Pendente' CHECK (status IN ('Pendente', 'Pago')),
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE (cliente_id, data_competencia)
);

CREATE INDEX IF NOT EXISTS idx_receitas_cliente ON receitas (cliente_id);
CREATE INDEX IF NOT EXISTS idx_receitas_competencia ON receitas (data_competencia);

CREATE TABLE IF NOT EXISTS cartoes_credito (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  nome TEXT NOT NULL,
  limite REAL CHECK (limite IS NULL OR limite > 0),
  melhor_dia_compra INTEGER CHECK (
    melhor_dia_compra IS NULL OR melhor_dia_compra BETWEEN 1 AND 31
  ),
  dia_vencimento INTEGER CHECK (
    dia_vencimento IS NULL OR dia_vencimento BETWEEN 1 AND 31
  ),
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  CONSTRAINT uq_cartoes_credito_nome UNIQUE (nome)
);

CREATE TABLE IF NOT EXISTS contas_bancarias (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  nome TEXT NOT NULL,
  instituicao TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  CONSTRAINT uq_contas_bancarias_nome UNIQUE (nome)
);

CREATE TABLE IF NOT EXISTS transacoes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  data TEXT NOT NULL,
  descricao TEXT NOT NULL,
  valor REAL NOT NULL,
  categoria TEXT NOT NULL DEFAULT 'Variável',
  cartao_id INTEGER REFERENCES cartoes_credito (id) ON DELETE SET NULL,
  conta_bancaria_id INTEGER REFERENCES contas_bancarias (id) ON DELETE SET NULL,
  mes_fatura TEXT,
  parcela_atual INTEGER CHECK (parcela_atual IS NULL OR parcela_atual >= 1),
  parcela_total INTEGER CHECK (parcela_total IS NULL OR parcela_total >= 1),
  realizado INTEGER NOT NULL DEFAULT 1 CHECK (realizado IN (0, 1)),
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  CONSTRAINT ck_transacao_origem CHECK (
    (cartao_id IS NULL AND conta_bancaria_id IS NULL)
    OR (cartao_id IS NOT NULL AND conta_bancaria_id IS NULL)
    OR (cartao_id IS NULL AND conta_bancaria_id IS NOT NULL)
  ),
  CONSTRAINT ck_transacao_parcela CHECK (
    parcela_atual IS NULL
    OR parcela_total IS NULL
    OR parcela_atual <= parcela_total
  )
);

CREATE INDEX IF NOT EXISTS idx_transacoes_data ON transacoes (data);
CREATE INDEX IF NOT EXISTS idx_transacoes_cartao ON transacoes (cartao_id);
CREATE INDEX IF NOT EXISTS idx_transacoes_conta ON transacoes (conta_bancaria_id);
CREATE INDEX IF NOT EXISTS idx_transacoes_mes_fatura ON transacoes (mes_fatura);

CREATE TABLE IF NOT EXISTS dividas_emprestimos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tipo TEXT NOT NULL,
  instituicao TEXT NOT NULL,
  descricao TEXT NOT NULL,
  valor_total REAL NOT NULL CHECK (valor_total >= 0),
  taxa_juros_mensal_pct REAL CHECK (
    taxa_juros_mensal_pct IS NULL OR taxa_juros_mensal_pct >= 0
  ),
  taxa_implicita INTEGER NOT NULL DEFAULT 0 CHECK (taxa_implicita IN (0, 1)),
  valor_parcela REAL NOT NULL CHECK (valor_parcela >= 0),
  parcelas_restantes INTEGER CHECK (
    parcelas_restantes IS NULL OR parcelas_restantes >= 0
  ),
  saldo_quitacao REAL,
  prioridade INTEGER,
  ativo INTEGER NOT NULL DEFAULT 1 CHECK (ativo IN (0, 1)),
  termino_previsto TEXT,
  observacoes TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_dividas_prioridade ON dividas_emprestimos (prioridade);

CREATE TABLE IF NOT EXISTS fluxo_mensal_snapshot (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  mes_ano TEXT NOT NULL,
  payload TEXT NOT NULL,
  fonte TEXT NOT NULL DEFAULT 'planilha',
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  CONSTRAINT uq_fluxo_mes_fonte UNIQUE (mes_ano, fonte)
);

-- Entradas fora do fluxo recorrente (aportes, empréstimos, resgates, reembolsos).
CREATE TABLE IF NOT EXISTS entradas_extras (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  data TEXT NOT NULL,
  descricao TEXT NOT NULL,
  valor REAL NOT NULL,
  categoria TEXT NOT NULL,
  origem TEXT,
  status TEXT NOT NULL DEFAULT 'Realizado'
);

CREATE INDEX IF NOT EXISTS idx_entradas_extras_data ON entradas_extras (data);

-- Fechamento de fatura por cartão (gestão de liquidação).
CREATE TABLE IF NOT EXISTS faturas_pagas (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  cartao_id INTEGER NOT NULL REFERENCES cartoes_credito (id) ON DELETE CASCADE,
  mes_referencia TEXT NOT NULL,
  data_vencimento TEXT NOT NULL,
  valor_total REAL NOT NULL CHECK (valor_total >= 0),
  status_pago INTEGER NOT NULL DEFAULT 0 CHECK (status_pago IN (0, 1)),
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE (cartao_id, mes_referencia)
);

CREATE INDEX IF NOT EXISTS idx_faturas_venc ON faturas_pagas (data_vencimento);
CREATE INDEX IF NOT EXISTS idx_faturas_cartao ON faturas_pagas (cartao_id);

-- Amortização manual (aba Dívidas; não entra automaticamente no fluxo).
CREATE TABLE IF NOT EXISTS pagamentos_dividas (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  divida_id INTEGER NOT NULL REFERENCES dividas_emprestimos (id) ON DELETE CASCADE,
  data_pagamento TEXT NOT NULL,
  valor REAL NOT NULL CHECK (valor > 0),
  observacao TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_pagamentos_divida ON pagamentos_dividas (divida_id);

-- Despesas futuras (provisionamento); ao realizar, gera linha em transacoes.
CREATE TABLE IF NOT EXISTS despesas_provisionadas (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  data_prevista TEXT NOT NULL,
  descricao TEXT NOT NULL,
  valor_previsto REAL NOT NULL CHECK (valor_previsto >= 0),
  categoria TEXT NOT NULL DEFAULT 'Variável',
  realizado INTEGER NOT NULL DEFAULT 0 CHECK (realizado IN (0, 1)),
  transacao_id INTEGER REFERENCES transacoes (id) ON DELETE SET NULL,
  data_realizada TEXT,
  valor_real REAL,
  conta_bancaria_id INTEGER REFERENCES contas_bancarias (id) ON DELETE SET NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_desp_prov_prev ON despesas_provisionadas (data_prevista);
CREATE INDEX IF NOT EXISTS idx_desp_prov_real ON despesas_provisionadas (realizado);
"""


# ── PostgreSQL compatibility layer ────────────────────────────────────────────

def _sqlite_to_pg(sql: str) -> str:
    """Translata sintaxe SQLite → PostgreSQL para uso com psycopg2."""
    # 1. Placeholders: ? → %s
    sql = sql.replace("?", "%s")
    # 2. strftime('%Y-%m', col) → TO_CHAR(col, 'YYYY-MM')
    sql = _re.sub(
        r"strftime\s*\(\s*'%Y-%m'\s*,\s*(\w+)\s*\)",
        r"TO_CHAR(\1, 'YYYY-MM')",
        sql, flags=_re.IGNORECASE,
    )
    # 3. date(%s) → %s::date  (parâmetro)
    sql = _re.sub(r"\bdate\(%s\)", "%s::date", sql, flags=_re.IGNORECASE)
    # 4. date(col) → col::date  (coluna literal)
    sql = _re.sub(r"\bdate\((\w+)\)", r"\1::date", sql, flags=_re.IGNORECASE)
    # 5. datetime('now') → NOW()
    sql = _re.sub(r"datetime\('now'\)", "NOW()", sql, flags=_re.IGNORECASE)
    # 6. COLLATE NOCASE → (sem suporte nativo; PostgreSQL é case-sensitive por padrão)
    sql = _re.sub(r"\s+COLLATE\s+NOCASE\b", "", sql, flags=_re.IGNORECASE)
    # 7. last_insert_rowid() → lastval()  (PostgreSQL usa sequências SERIAL)
    sql = _re.sub(r"\blast_insert_rowid\(\)", "lastval()", sql, flags=_re.IGNORECASE)
    return sql


class _PgRow:
    """Adapta psycopg2 RealDictRow para se comportar como sqlite3.Row."""
    __slots__ = ("_d", "_keys")

    def __init__(self, data: dict) -> None:
        self._d = dict(data)
        self._keys = list(data.keys())

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._d[self._keys[key]]
        return self._d[key]

    def __iter__(self):
        return iter(self._d.values())

    def keys(self):
        return self._keys

    def get(self, key, default=None):
        return self._d.get(key, default)


class _PgCursor:
    """Envolve cursor psycopg2 imitando sqlite3.Cursor."""

    def __init__(self, cur) -> None:
        self._cur = cur

    def fetchone(self):
        row = self._cur.fetchone()
        return _PgRow(row) if row is not None else None

    def fetchall(self):
        return [_PgRow(r) for r in (self._cur.fetchall() or [])]

    def __iter__(self):
        for row in self._cur:
            yield _PgRow(row)

    @property
    def description(self):
        return self._cur.description

    @property
    def rowcount(self) -> int:
        return self._cur.rowcount


class PgConn:
    """Envolve psycopg2 connection imitando sqlite3.Connection."""

    def __init__(self, pg_conn) -> None:
        self._conn = pg_conn

    def execute(self, sql: str, params=()):
        tsql = _sqlite_to_pg(sql)
        cur = self._conn.cursor(cursor_factory=_psycopg2_extras.RealDictCursor)
        cur.execute(tsql, params if params else None)
        return _PgCursor(cur)

    def executescript(self, sql: str) -> None:  # noqa: ARG002
        """No-op: schema gerenciado no Supabase."""

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()


def read_sql(sql: str, conn, params=None):
    """Executa SQL e retorna DataFrame — compatível com SQLite e PgConn.

    Substituto de pd.read_sql_query() que funciona com nosso wrapper PgConn
    (psycopg2) sem depender de SQLAlchemy.
    """
    import pandas as _pd

    cur = conn.execute(sql, params) if params else conn.execute(sql)
    rows = cur.fetchall()

    # Obtém nomes das colunas
    if rows:
        cols = list(rows[0].keys())
        data = [[row[k] for k in cols] for row in rows]
    else:
        desc = getattr(cur, "description", None)
        cols = [d[0] for d in desc] if desc else []
        data = []

    return _pd.DataFrame(data, columns=cols)


def _read_pg_url() -> str | None:
    """Lê URL Supabase de secrets.toml — dev local ou Streamlit Cloud."""
    candidates = [
        BASE_DIR / ".streamlit" / "secrets.toml",    # dev local
        Path.home() / ".streamlit" / "secrets.toml", # Streamlit Cloud
    ]
    for secrets_path in candidates:
        if not secrets_path.exists():
            continue
        try:
            import toml  # já no venv
            data = toml.load(str(secrets_path))
            url = data.get("connections", {}).get("postgresql", {}).get("url")
            if url:
                return url
        except Exception:
            continue
    return None


def _pg_connect(url: str) -> PgConn:
    """Abre conexão psycopg2 com Supabase e retorna PgConn pronto para uso."""
    pg = _psycopg2.connect(url)
    pg.autocommit = False
    conn = PgConn(pg)
    ensure_entradas_extras_status_column(conn)
    return conn


# ── Fim do bloco PostgreSQL ───────────────────────────────────────────────────


def _drop_all_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA foreign_keys = OFF;
        DROP TABLE IF EXISTS despesas_provisionadas;
        DROP TABLE IF EXISTS pagamentos_dividas;
        DROP TABLE IF EXISTS transacoes;
        DROP TABLE IF EXISTS faturas_pagas;
        DROP TABLE IF EXISTS receitas;
        DROP TABLE IF EXISTS dividas_emprestimos;
        DROP TABLE IF EXISTS entradas_extras;
        DROP TABLE IF EXISTS fluxo_mensal_snapshot;
        DROP TABLE IF EXISTS clientes;
        DROP TABLE IF EXISTS cartoes_credito;
        DROP TABLE IF EXISTS contas_bancarias;
        DROP TABLE IF EXISTS parametros_financeiros;
        PRAGMA foreign_keys = ON;
        """
    )


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _blank_to_none(cell: object | None) -> str | None:
    """Trata strings vazias ou só espaço como NULL no SQLite."""
    if cell is None:
        return None
    t = str(cell).strip()
    return t if t else None


def _parse_bool(cell: str | None) -> int | None:
    if cell is None or cell == "":
        return None
    s = cell.strip().lower()
    if s in ("1", "true", "t", "yes", "sim"):
        return 1
    if s in ("0", "false", "f", "no", "não", "nao"):
        return 0
    return None


def _to_float(cell: str | None) -> float | None:
    if cell is None or cell == "":
        return None
    return float(cell.replace(",", "."))


def _to_int(cell: str | None) -> int | None:
    if cell is None or cell == "":
        return None
    return int(float(cell.replace(",", ".")))


def _resolve_fk_to_int(
    raw: object | None,
    id_map: dict[str, int],
) -> int | None:
    """Converte FK do CSV: vazio → NULL; número → int; UUID/texto → id_map."""
    cell = _blank_to_none(raw)
    if cell is None:
        return None
    s = str(cell).strip()
    if s.isdigit():
        return int(s)
    return id_map.get(s)


def _categoria_transacao(raw: object | None) -> str:
    cell = _blank_to_none(raw)
    if cell is None:
        return "Variável"
    c = str(cell).strip()
    return c[:200] if c else "Variável"


def _normalizar_mes_fatura(raw: object | None) -> str | None:
    """YYYY-MM ou None."""
    cell = _blank_to_none(raw)
    if cell is None:
        return None
    s = str(cell).strip()[:10]
    if len(s) >= 7 and s[4] == "-":
        return s[:7]
    return None


def ensure_receitas_transacao_id_column(conn) -> None:
    """Garante coluna `transacao_id` em receitas (migração)."""
    if isinstance(conn, PgConn):
        row = conn.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = 'receitas' AND column_name = 'transacao_id'"
        ).fetchone()
        if row:
            return  # já existe — não toca na transação
        conn.execute("ALTER TABLE receitas ADD COLUMN transacao_id INTEGER")
        conn.commit()
    else:
        cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(receitas)").fetchall()}
        if "transacao_id" not in cols:
            conn.execute("ALTER TABLE receitas ADD COLUMN transacao_id INTEGER")
            conn.commit()


def ensure_receitas_data_prevista_column(conn) -> None:
    """Garante coluna `data_prevista_recebimento` em receitas (migração)."""
    if isinstance(conn, PgConn):
        row = conn.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = 'receitas' AND column_name = 'data_prevista_recebimento'"
        ).fetchone()
        if row:
            return
    else:
        cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(receitas)").fetchall()}
        if "data_prevista_recebimento" in cols:
            return
    conn.execute("ALTER TABLE receitas ADD COLUMN data_prevista_recebimento TEXT")
    conn.commit()


def ensure_entradas_extras_status_column(conn) -> None:
    """Garante coluna `status` (Realizado | Provisionado) em bases antigas."""
    if isinstance(conn, PgConn):
        row = conn.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = 'entradas_extras' AND column_name = 'status'"
        ).fetchone()
        if row:
            return
    else:
        cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(entradas_extras)").fetchall()}
        if "status" in cols:
            return
    conn.execute(
        """
        ALTER TABLE entradas_extras
        ADD COLUMN status TEXT NOT NULL DEFAULT 'Realizado'
        """
    )
    conn.commit()


def init_schema(conn) -> None:
    if isinstance(conn, PgConn):
        # Tabelas já gerenciadas no Supabase; apenas garante colunas de migração.
        ensure_entradas_extras_status_column(conn)
        ensure_receitas_transacao_id_column(conn)
        ensure_receitas_data_prevista_column(conn)
        return
    row = conn.execute("PRAGMA user_version").fetchone()
    v = int(row[0]) if row else 0
    if v < SCHEMA_VERSION:
        _drop_all_tables(conn)
        conn.executescript(SCHEMA_SQL)
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    else:
        conn.executescript(SCHEMA_SQL)
    conn.commit()
    ensure_entradas_extras_status_column(conn)


def _clear_data_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DELETE FROM despesas_provisionadas;
        DELETE FROM pagamentos_dividas;
        DELETE FROM transacoes;
        DELETE FROM faturas_pagas;
        DELETE FROM receitas;
        DELETE FROM dividas_emprestimos;
        DELETE FROM clientes;
        DELETE FROM contas_bancarias;
        DELETE FROM cartoes_credito;
        DELETE FROM parametros_financeiros;
        DELETE FROM fluxo_mensal_snapshot;
        DELETE FROM entradas_extras;
        """
    )
    conn.commit()


def load_parametros(conn: sqlite3.Connection, path: Path) -> None:
    with path.open(encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            vig = str(row["vigencia_inicio"]).strip()
            cdi = float(row["cdi_aa"])
            usd = _to_float(row.get("usd_brl"))
            obs = _blank_to_none(row.get("observacao"))
            cell = _blank_to_none(row.get("id"))
            if cell is None:
                conn.execute(
                    """
                    INSERT INTO parametros_financeiros
                    (vigencia_inicio, cdi_aa, usd_brl, observacao)
                    VALUES (?, ?, ?, ?)
                    """,
                    (vig, cdi, usd, obs),
                )
            elif str(cell).strip().isdigit():
                conn.execute(
                    """
                    INSERT OR REPLACE INTO parametros_financeiros
                    (id, vigencia_inicio, cdi_aa, usd_brl, observacao)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (int(str(cell).strip()), vig, cdi, usd, obs),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO parametros_financeiros
                    (vigencia_inicio, cdi_aa, usd_brl, observacao)
                    VALUES (?, ?, ?, ?)
                    """,
                    (vig, cdi, usd, obs),
                )


def load_cartoes(conn: sqlite3.Connection, path: Path, id_map: dict[str, int]) -> None:
    with path.open(encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            nome = str(row["nome"]).strip()
            lim = _to_float(row.get("limite"))
            md = _to_int(row.get("melhor_dia_compra"))
            dv = _to_int(row.get("dia_vencimento"))
            cell = _blank_to_none(row.get("id"))
            if cell is None:
                conn.execute(
                    """
                    INSERT INTO cartoes_credito (nome, limite, melhor_dia_compra, dia_vencimento)
                    VALUES (?, ?, ?, ?)
                    """,
                    (nome, lim, md, dv),
                )
                rid = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                id_map[str(rid)] = rid
            elif str(cell).strip().isdigit():
                i = int(str(cell).strip())
                conn.execute(
                    """
                    INSERT OR REPLACE INTO cartoes_credito
                    (id, nome, limite, melhor_dia_compra, dia_vencimento)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (i, nome, lim, md, dv),
                )
                id_map[str(i)] = i
            else:
                u = str(cell).strip()
                conn.execute(
                    """
                    INSERT INTO cartoes_credito (nome, limite, melhor_dia_compra, dia_vencimento)
                    VALUES (?, ?, ?, ?)
                    """,
                    (nome, lim, md, dv),
                )
                rid = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                id_map[u] = rid


def load_contas(conn: sqlite3.Connection, path: Path, id_map: dict[str, int]) -> None:
    with path.open(encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            nome = str(row["nome"]).strip()
            inst = _blank_to_none(row.get("instituicao"))
            cell = _blank_to_none(row.get("id"))
            if cell is None:
                conn.execute(
                    "INSERT INTO contas_bancarias (nome, instituicao) VALUES (?, ?)",
                    (nome, inst),
                )
                rid = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                id_map[str(rid)] = rid
            elif str(cell).strip().isdigit():
                i = int(str(cell).strip())
                conn.execute(
                    """
                    INSERT OR REPLACE INTO contas_bancarias (id, nome, instituicao)
                    VALUES (?, ?, ?)
                    """,
                    (i, nome, inst),
                )
                id_map[str(i)] = i
            else:
                u = str(cell).strip()
                conn.execute(
                    "INSERT INTO contas_bancarias (nome, instituicao) VALUES (?, ?)",
                    (nome, inst),
                )
                rid = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                id_map[u] = rid


def load_clientes_completo(
    conn: sqlite3.Connection,
    path: Path,
    id_map: dict[str, int],
) -> None:
    if path.name != "clientes_completo.csv":
        raise ValueError("Carga de clientes permitida somente via clientes_completo.csv")
    with path.open(encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            nome = str(row["nome"]).strip()
            vh = float(str(row["valor_honorario"]).replace(",", "."))
            dv = int(float(str(row["dia_vencimento"]).replace(",", ".")))
            st = str(row["status"]).strip()
            vig = _blank_to_none(row.get("honorario_vigencia_inicio"))
            pont = _blank_to_none(row.get("pontualidade"))
            obs = _blank_to_none(row.get("observacao"))
            cell = _blank_to_none(row.get("id"))
            if cell is None:
                conn.execute(
                    """
                    INSERT INTO clientes (
                      nome, valor_honorario, dia_vencimento, status,
                      honorario_vigencia_inicio, pontualidade, observacao
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (nome, vh, dv, st, vig, pont, obs),
                )
                rid = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                id_map[str(rid)] = rid
            elif str(cell).strip().isdigit():
                i = int(str(cell).strip())
                conn.execute(
                    """
                    INSERT OR REPLACE INTO clientes (
                      id, nome, valor_honorario, dia_vencimento, status,
                      honorario_vigencia_inicio, pontualidade, observacao
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (i, nome, vh, dv, st, vig, pont, obs),
                )
                id_map[str(i)] = i
            else:
                u = str(cell).strip()
                conn.execute(
                    """
                    INSERT INTO clientes (
                      nome, valor_honorario, dia_vencimento, status,
                      honorario_vigencia_inicio, pontualidade, observacao
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (nome, vh, dv, st, vig, pont, obs),
                )
                rid = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                id_map[u] = rid


def load_dividas(conn: sqlite3.Connection, path: Path) -> None:
    with path.open(encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            ti = _parse_bool(row.get("taxa_implicita"))
            if ti is None:
                ti = 0
            ativo = _parse_bool(row.get("ativo"))
            if ativo is None:
                ativo = 1
            tipo = str(row["tipo"]).strip()
            inst = str(row["instituicao"]).strip()
            desc = str(row["descricao"]).strip()
            vt = float(str(row["valor_total"]).replace(",", "."))
            tj = _to_float(row.get("taxa_juros_mensal_pct"))
            vp = float(str(row["valor_parcela"]).replace(",", "."))
            pr = _to_int(row.get("parcelas_restantes"))
            sq = _to_float(row.get("saldo_quitacao"))
            prio = _to_int(row.get("prioridade"))
            term = _blank_to_none(row.get("termino_previsto"))
            obs = _blank_to_none(row.get("observacoes"))
            cell = _blank_to_none(row.get("id"))
            if cell is None:
                conn.execute(
                    """
                    INSERT INTO dividas_emprestimos (
                      tipo, instituicao, descricao, valor_total,
                      taxa_juros_mensal_pct, taxa_implicita, valor_parcela,
                      parcelas_restantes, saldo_quitacao, prioridade, ativo,
                      termino_previsto, observacoes
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        tipo,
                        inst,
                        desc,
                        vt,
                        tj,
                        ti,
                        vp,
                        pr,
                        sq,
                        prio,
                        ativo,
                        term,
                        obs,
                    ),
                )
            elif str(cell).strip().isdigit():
                i = int(str(cell).strip())
                conn.execute(
                    """
                    INSERT OR REPLACE INTO dividas_emprestimos (
                      id, tipo, instituicao, descricao, valor_total,
                      taxa_juros_mensal_pct, taxa_implicita, valor_parcela,
                      parcelas_restantes, saldo_quitacao, prioridade, ativo,
                      termino_previsto, observacoes
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        i,
                        tipo,
                        inst,
                        desc,
                        vt,
                        tj,
                        ti,
                        vp,
                        pr,
                        sq,
                        prio,
                        ativo,
                        term,
                        obs,
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO dividas_emprestimos (
                      tipo, instituicao, descricao, valor_total,
                      taxa_juros_mensal_pct, taxa_implicita, valor_parcela,
                      parcelas_restantes, saldo_quitacao, prioridade, ativo,
                      termino_previsto, observacoes
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        tipo,
                        inst,
                        desc,
                        vt,
                        tj,
                        ti,
                        vp,
                        pr,
                        sq,
                        prio,
                        ativo,
                        term,
                        obs,
                    ),
                )


def load_receitas(
    conn: sqlite3.Connection,
    path: Path,
    map_clientes: dict[str, int],
) -> None:
    with path.open(encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            cid_raw = str(row["cliente_id"]).strip()
            cliente_id = _resolve_fk_to_int(cid_raw, map_clientes)
            if cliente_id is None:
                continue
            comp = str(row["data_competencia"]).strip()
            dr = _blank_to_none(row.get("data_recebimento_real"))
            st = str(row["status"]).strip()
            conn.execute(
                """
                INSERT INTO receitas (
                  cliente_id, data_competencia, data_recebimento_real, status
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT (cliente_id, data_competencia) DO UPDATE SET
                  status = excluded.status,
                  data_recebimento_real = excluded.data_recebimento_real
                """,
                (cliente_id, comp, dr, st),
            )


def load_transacoes(
    conn: sqlite3.Connection,
    path: Path,
    map_cartoes: dict[str, int],
    map_contas: dict[str, int],
) -> None:
    with path.open(encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            real = _parse_bool(row.get("realizado"))
            if real is None:
                real = 1
            cat = _categoria_transacao(row.get("categoria"))
            data = str(row["data"]).strip()
            desc = str(row["descricao"]).strip()
            valor = float(str(row["valor"]).replace(",", "."))
            cartao_id = _resolve_fk_to_int(row.get("cartao_id"), map_cartoes)
            conta_id = _resolve_fk_to_int(row.get("conta_bancaria_id"), map_contas)
            pa = _to_int(row.get("parcela_atual"))
            pt = _to_int(row.get("parcela_total"))
            mes_fat = _normalizar_mes_fatura(row.get("mes_fatura"))
            cell = _blank_to_none(row.get("id"))
            if cell is None:
                conn.execute(
                    """
                    INSERT INTO transacoes (
                      data, descricao, valor, categoria,
                      cartao_id, conta_bancaria_id, mes_fatura,
                      parcela_atual, parcela_total,
                      realizado
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        data,
                        desc,
                        valor,
                        cat,
                        cartao_id,
                        conta_id,
                        mes_fat,
                        pa,
                        pt,
                        real,
                    ),
                )
            elif str(cell).strip().isdigit():
                i = int(str(cell).strip())
                conn.execute(
                    """
                    INSERT OR REPLACE INTO transacoes (
                      id, data, descricao, valor, categoria,
                      cartao_id, conta_bancaria_id, mes_fatura,
                      parcela_atual, parcela_total,
                      realizado
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        i,
                        data,
                        desc,
                        valor,
                        cat,
                        cartao_id,
                        conta_id,
                        mes_fat,
                        pa,
                        pt,
                        real,
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO transacoes (
                      data, descricao, valor, categoria,
                      cartao_id, conta_bancaria_id, mes_fatura,
                      parcela_atual, parcela_total,
                      realizado
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        data,
                        desc,
                        valor,
                        cat,
                        cartao_id,
                        conta_id,
                        mes_fat,
                        pa,
                        pt,
                        real,
                    ),
                )


def import_csvs(conn: sqlite3.Connection) -> None:
    """Ordem conforme import/csv/_ordem_carga.txt; clientes só de clientes_completo.csv."""
    load_parametros(conn, CSV_DIR / "parametros_financeiros.csv")
    map_cartoes: dict[str, int] = {}
    load_cartoes(conn, CSV_DIR / "cartoes_credito.csv", map_cartoes)
    map_contas: dict[str, int] = {}
    load_contas(conn, CSV_DIR / "contas_bancarias.csv", map_contas)
    map_clientes: dict[str, int] = {}
    load_clientes_completo(conn, CLIENTES_CSV, map_clientes)
    load_dividas(conn, CSV_DIR / "dividas_emprestimos.csv")
    load_receitas(conn, CSV_DIR / "receitas.csv", map_clientes)
    load_transacoes(conn, CSV_DIR / "transacoes.csv", map_cartoes, map_contas)
    conn.commit()


def init_database(*, force_reload: bool = False):
    """
    Conecta ao banco de dados.
    - Supabase (PostgreSQL): lê URL de .streamlit/secrets.toml — tabelas gerenciadas remotamente.
    - SQLite local (fallback): cria arquivo, aplica schema e importa CSVs.
    `force_reload=True` recarrega CSVs apenas no modo SQLite.
    """
    pg_url = _read_pg_url()
    if pg_url:
        return _pg_connect(pg_url)
    # ── SQLite local ──────────────────────────────────────────────────────────
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = connect()
    init_schema(conn)
    if force_reload or not _has_any_cliente(conn):
        _clear_data_tables(conn)
        import_csvs(conn)
    return conn


def _has_any_cliente(conn: sqlite3.Connection) -> bool:
    cur = conn.execute("SELECT 1 FROM clientes LIMIT 1")
    return cur.fetchone() is not None


def faturamento_mensal_total(conn: sqlite3.Connection) -> float:
    """Soma honorários dos clientes ativos (carteira plena ≈ R$ 6.330,00)."""
    row = conn.execute(
        """
        SELECT COALESCE(SUM(valor_honorario), 0) AS t
        FROM clientes
        WHERE status = 'Ativo'
        """
    ).fetchone()
    return float(row["t"])


def saldo_transacoes(conn: sqlite3.Connection) -> float:
    """Soma das transações **realizadas** (saldo de caixa / exclui `realizado = 0`)."""
    row = conn.execute(
        "SELECT COALESCE(SUM(valor), 0) AS s FROM transacoes WHERE realizado = 1"
    ).fetchone()
    return float(row["s"])


def total_entradas_extras(conn: sqlite3.Connection) -> float:
    """Soma em `entradas_extras` já **realizadas** (exclui `Provisionado`)."""
    row = conn.execute(
        """
        SELECT COALESCE(SUM(valor), 0) AS s FROM entradas_extras
        WHERE COALESCE(status, 'Realizado') = 'Realizado'
        """
    ).fetchone()
    return float(row["s"])


def total_entradas_extras_com_provisoes(conn: sqlite3.Connection) -> float:
    """Soma em `entradas_extras` — inclui **Realizado** e **Provisionado**."""
    row = conn.execute(
        "SELECT COALESCE(SUM(valor), 0) AS s FROM entradas_extras"
    ).fetchone()
    return float(row["s"])


def total_entradas_extras_so_provisionadas(conn: sqlite3.Connection) -> float:
    """Soma de entradas extras com status **Provisionado** (ainda a receber)."""
    row = conn.execute(
        """
        SELECT COALESCE(SUM(valor), 0) AS s FROM entradas_extras
        WHERE COALESCE(status, 'Realizado') = 'Provisionado'
        """
    ).fetchone()
    return float(row["s"])


def count_entradas_extras_provisionadas(conn: sqlite3.Connection) -> int:
    """Quantidade de entradas extras com status **Provisionado**."""
    row = conn.execute(
        """
        SELECT COUNT(*) AS n FROM entradas_extras
        WHERE COALESCE(status, 'Realizado') = 'Provisionado'
        """
    ).fetchone()
    return int(row["n"])


def total_despesas_provisionadas_nao_realizadas(conn: sqlite3.Connection) -> float:
    """Soma de `valor_previsto` em `despesas_provisionadas` ainda **não realizadas**."""
    row = conn.execute(
        """
        SELECT COALESCE(SUM(valor_previsto), 0) AS s
        FROM despesas_provisionadas
        WHERE realizado = 0
        """
    ).fetchone()
    return float(row["s"])


def total_entradas_extras_mes_categorias(
    conn: sqlite3.Connection,
    ym: str,
    categorias: tuple[str, ...],
) -> float:
    """Soma `entradas_extras` no mês `ym` (YYYY-MM) para as categorias indicadas."""
    if not categorias:
        return 0.0
    ph = ",".join("?" * len(categorias))
    row = conn.execute(
        f"""
        SELECT COALESCE(SUM(valor), 0) AS s FROM entradas_extras
        WHERE strftime('%Y-%m', data) = ?
          AND COALESCE(status, 'Realizado') = 'Realizado'
          AND categoria IN ({ph})
        """,
        (ym, *categorias),
    ).fetchone()
    return float(row["s"])


def saldo_caixa_total(conn: sqlite3.Connection) -> float:
    """Saldo de caixa: transações **realizadas** (`realizado = 1`) + entradas extras **Realizado**."""
    return saldo_transacoes(conn) + total_entradas_extras(conn)


def saldo_caixa_previsto(conn: sqlite3.Connection) -> float:
    """
    Saldo projetado completo:
      Saldo Real (transações realizadas + extras Realizado)
      + Entradas Extras Provisionadas (a receber)
      - Despesas Provisionadas não realizadas (a pagar)
    """
    return (
        saldo_caixa_total(conn)
        + total_entradas_extras_so_provisionadas(conn)
        - total_despesas_provisionadas_nao_realizadas(conn)
    )


def saldo_caixa_ate_data(conn: sqlite3.Connection, data_inicio: date) -> float:
    """Saldo até antes de `data_inicio`: transações realizadas + extras **Realizado** apenas."""
    ds = data_inicio.isoformat()
    row = conn.execute(
        """
        SELECT COALESCE(SUM(valor), 0) AS s FROM transacoes
        WHERE realizado = 1
          AND date(data) < date(?)
        """,
        (ds,),
    ).fetchone()
    s_tr = float(row["s"])
    row2 = conn.execute(
        """
        SELECT COALESCE(SUM(valor), 0) AS s FROM entradas_extras
        WHERE COALESCE(status, 'Realizado') = 'Realizado'
          AND date(data) < date(?)
        """,
        (ds,),
    ).fetchone()
    return s_tr + float(row2["s"])


def insert_entrada_extra(
    conn: sqlite3.Connection,
    *,
    data: str,
    descricao: str,
    valor: float,
    categoria: str,
    origem: str | None,
    status: str = "Realizado",
) -> None:
    st = str(status).strip()
    if st not in ("Realizado", "Provisionado"):
        st = "Realizado"
    conn.execute(
        """
        INSERT INTO entradas_extras (data, descricao, valor, categoria, origem, status)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (data, descricao, float(valor), categoria, origem, st),
    )
    conn.commit()


def update_entrada_extra(
    conn: sqlite3.Connection,
    row_id: int,
    *,
    data: str,
    descricao: str,
    valor: float,
    categoria: str,
    origem: str | None,
    status: str = "Realizado",
) -> None:
    st = str(status).strip()
    if st not in ("Realizado", "Provisionado"):
        st = "Realizado"
    conn.execute(
        """
        UPDATE entradas_extras
        SET data = ?, descricao = ?, valor = ?, categoria = ?, origem = ?, status = ?
        WHERE id = ?
        """,
        (data, descricao, float(valor), categoria, origem, st, int(row_id)),
    )
    conn.commit()


def receber_entrada_extra_provisionada(
    conn: sqlite3.Connection,
    row_id: int,
    *,
    data_recebimento_iso: str | None = None,
) -> None:
    """Marca extra como **Realizado** e opcionalmente ajusta a data ao recebimento."""
    row = conn.execute(
        """
        SELECT id FROM entradas_extras
        WHERE id = ? AND COALESCE(status, 'Realizado') = 'Provisionado'
        """,
        (int(row_id),),
    ).fetchone()
    if not row:
        raise ValueError("Lançamento inexistente ou já realizado.")
    ds = (
        str(data_recebimento_iso).strip()[:10]
        if data_recebimento_iso
        else date.today().isoformat()
    )
    conn.execute(
        """
        UPDATE entradas_extras
        SET status = 'Realizado', data = ?
        WHERE id = ?
        """,
        (ds, int(row_id)),
    )
    conn.commit()


def delete_entrada_extra(conn: sqlite3.Connection, row_id: int) -> None:
    conn.execute("DELETE FROM entradas_extras WHERE id = ?", (int(row_id),))
    conn.commit()


def insert_cliente(
    conn,
    *,
    nome: str,
    valor_honorario: float,
    dia_vencimento: int,
    honorario_vigencia_inicio: str | None = None,
    observacao: str | None = None,
) -> int:
    """Insere novo cliente e retorna o id gerado."""
    vig = honorario_vigencia_inicio[:10] if honorario_vigencia_inicio else None
    obs = observacao.strip() if observacao and observacao.strip() else None
    try:
        cur = conn.execute(
            """
            INSERT INTO clientes (nome, valor_honorario, dia_vencimento, status,
                                  honorario_vigencia_inicio, observacao)
            VALUES (?, ?, ?, 'Ativo', ?, ?)
            """,
            (nome.strip(), float(valor_honorario), int(dia_vencimento), vig, obs),
        )
        conn.commit()
        return int(cur.lastrowid or 0)
    except Exception:
        # fallback para PostgreSQL (lastrowid não funciona sempre)
        conn.commit()
        row = conn.execute(
            "SELECT id FROM clientes WHERE nome = ? ORDER BY id DESC LIMIT 1",
            (nome.strip(),),
        ).fetchone()
        return int(row["id"]) if row else 0


def update_cliente(
    conn,
    *,
    cliente_id: int,
    nome: str,
    valor_honorario: float,
    dia_vencimento: int,
    honorario_vigencia_inicio: str | None = None,
    observacao: str | None = None,
) -> None:
    """Atualiza dados de um cliente existente."""
    vig = honorario_vigencia_inicio[:10] if honorario_vigencia_inicio else None
    obs = observacao.strip() if observacao and observacao.strip() else None
    conn.execute(
        """
        UPDATE clientes
        SET nome = ?, valor_honorario = ?, dia_vencimento = ?,
            honorario_vigencia_inicio = ?, observacao = ?
        WHERE id = ?
        """,
        (nome.strip(), float(valor_honorario), int(dia_vencimento), vig, obs, int(cliente_id)),
    )
    conn.commit()


def inativar_cliente(conn, *, cliente_id: int) -> None:
    """Marca cliente como Inativo."""
    conn.execute(
        "UPDATE clientes SET status = 'Inativo' WHERE id = ?",
        (int(cliente_id),),
    )
    conn.commit()


def upsert_receita_mes(
    conn,
    *,
    cliente_id: int | str,
    data_competencia: str,
    status: str,
    data_recebimento: str | None = None,
    data_prevista_recebimento: str | None = None,
) -> None:
    """Atualiza ou cria receita do mês e espelha em transacoes (quando possível)."""
    if status not in ("Pendente", "Pago", "Isento"):
        raise ValueError("status inválido")

    # "Isento" é salvo como Pendente com marcador especial em data_prevista_recebimento.
    # Evita alterar a CHECK constraint do Supabase.
    if status == "Isento":
        status = "Pendente"
        data_prevista_recebimento = "ISENTO"

    cid = int(cliente_id)
    data_rec = (data_recebimento or date.today().isoformat())[:10] if status == "Pago" else None

    # ── Tenta versão completa com transacao_id ────────────────────────────────
    try:
        if status == "Pago":
            row_cli = conn.execute(
                "SELECT nome, valor_honorario FROM clientes WHERE id = ?", (cid,)
            ).fetchone()
            nome_cli = str(row_cli["nome"]) if row_cli else f"Cliente {cid}"
            valor_hon = float(row_cli["valor_honorario"]) if row_cli else 0.0

            ym = data_competencia[:7]
            _M = {"01":"jan","02":"fev","03":"mar","04":"abr","05":"mai","06":"jun",
                  "07":"jul","08":"ago","09":"set","10":"out","11":"nov","12":"dez"}
            desc = f"Honorário {nome_cli} ({_M.get(ym[5:],'?')}/{ym[:4]})"

            row_rec = conn.execute(
                "SELECT transacao_id FROM receitas WHERE cliente_id = ? AND data_competencia = ?",
                (cid, data_competencia),
            ).fetchone()
            tid_atual = int(row_rec["transacao_id"]) if (row_rec and row_rec["transacao_id"]) else None

            if tid_atual:
                conn.execute(
                    "UPDATE transacoes SET data = ?, valor = ? WHERE id = ?",
                    (data_rec, valor_hon, tid_atual),
                )
                new_tid = tid_atual
            else:
                conn.execute(
                    "INSERT INTO transacoes (data, descricao, valor, categoria, realizado)"
                    " VALUES (?, ?, ?, 'Honorários', 1)",
                    (data_rec, desc, valor_hon),
                )
                new_tid = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

            conn.execute(
                """
                INSERT INTO receitas (cliente_id, data_competencia, data_recebimento_real, status, transacao_id)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (cliente_id, data_competencia) DO UPDATE SET
                  status                = excluded.status,
                  data_recebimento_real = excluded.data_recebimento_real,
                  transacao_id          = excluded.transacao_id
                """,
                (cid, data_competencia, data_rec, status, new_tid),
            )

        else:  # Pendente
            row_rec = conn.execute(
                "SELECT transacao_id FROM receitas WHERE cliente_id = ? AND data_competencia = ?",
                (cid, data_competencia),
            ).fetchone()
            if row_rec and row_rec["transacao_id"]:
                conn.execute("DELETE FROM transacoes WHERE id = ?", (int(row_rec["transacao_id"]),))

            dp = data_prevista_recebimento[:10] if data_prevista_recebimento else None
            conn.execute(
                """
                INSERT INTO receitas (cliente_id, data_competencia, data_recebimento_real, status, transacao_id, data_prevista_recebimento)
                VALUES (?, ?, NULL, ?, NULL, ?)
                ON CONFLICT (cliente_id, data_competencia) DO UPDATE SET
                  status                      = excluded.status,
                  data_recebimento_real        = NULL,
                  transacao_id                = NULL,
                  data_prevista_recebimento   = excluded.data_prevista_recebimento
                """,
                (cid, data_competencia, status, dp),
            )

        conn.commit()
        return  # sucesso — sai aqui

    except Exception:
        # transacao_id ainda não existe no Supabase — rollback e usa versão simples
        try:
            conn.rollback()
        except Exception:
            pass

    # ── Fallback: versão sem transacao_id (enquanto coluna não existe) ────────
    if status == "Pago":
        # Lança em transacoes mesmo assim (data correta)
        try:
            row_cli = conn.execute(
                "SELECT nome, valor_honorario FROM clientes WHERE id = ?", (cid,)
            ).fetchone()
            valor_hon = float(row_cli["valor_honorario"]) if row_cli else 0.0
            nome_cli = str(row_cli["nome"]) if row_cli else f"Cliente {cid}"
            ym = data_competencia[:7]
            _M = {"01":"jan","02":"fev","03":"mar","04":"abr","05":"mai","06":"jun",
                  "07":"jul","08":"ago","09":"set","10":"out","11":"nov","12":"dez"}
            conn.execute(
                "INSERT INTO transacoes (data, descricao, valor, categoria, realizado)"
                " VALUES (?, ?, ?, 'Honorários', 1)",
                (data_rec, f"Honorário {nome_cli} ({_M.get(ym[5:],'?')}/{ym[:4]})", valor_hon),
            )
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass

    dp = data_prevista_recebimento[:10] if data_prevista_recebimento else None
    conn.execute(
        """
        INSERT INTO receitas (cliente_id, data_competencia, data_recebimento_real, status, data_prevista_recebimento)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (cliente_id, data_competencia) DO UPDATE SET
          status                    = excluded.status,
          data_recebimento_real     = excluded.data_recebimento_real,
          data_prevista_recebimento = excluded.data_prevista_recebimento
        """,
        (cid, data_competencia, data_rec, status, dp),
    )
    conn.commit()


def soma_fatura_cartao(conn: sqlite3.Connection, cartao_id: int | None) -> float:
    if cartao_id is None:
        return 0.0
    row = conn.execute(
        """
        SELECT COALESCE(SUM(valor), 0) AS s
        FROM transacoes
        WHERE cartao_id = ?
          AND realizado = 1
        """,
        (int(cartao_id),),
    ).fetchone()
    return float(row["s"])


def insert_transacao_simple(
    conn: sqlite3.Connection,
    *,
    data: str,
    descricao: str,
    valor: float,
    categoria: str = "Variável",
) -> int:
    """Lançamento só com data, descrição e valor (sem cartão/conta). Categoria livre."""
    cat = _categoria_transacao(categoria)
    conn.execute(
        """
        INSERT INTO transacoes (
          data, descricao, valor, categoria, cartao_id, conta_bancaria_id,
          mes_fatura, realizado
        )
        VALUES (?, ?, ?, ?, NULL, NULL, NULL, 1)
        """,
        (data, descricao, float(valor), cat),
    )
    conn.commit()
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def primeira_conta_id(conn: sqlite3.Connection) -> int | None:
    row = conn.execute(
        "SELECT id FROM contas_bancarias ORDER BY id LIMIT 1"
    ).fetchone()
    return int(row[0]) if row else None


def insert_cartao_credito(
    conn: sqlite3.Connection,
    *,
    nome: str,
    limite: float | None,
    dia_vencimento: int | None,
    melhor_dia_compra: int | None = None,
) -> int:
    nm = str(nome).strip()
    if not nm:
        raise ValueError("Nome do cartão é obrigatório.")
    conn.execute(
        """
        INSERT INTO cartoes_credito (nome, limite, melhor_dia_compra, dia_vencimento)
        VALUES (?, ?, ?, ?)
        """,
        (
            nm[:200],
            float(limite) if limite is not None and float(limite) > 0 else None,
            int(melhor_dia_compra) if melhor_dia_compra is not None else None,
            int(dia_vencimento) if dia_vencimento is not None else None,
        ),
    )
    conn.commit()
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def insert_despesa_debito(
    conn: sqlite3.Connection,
    *,
    data: str,
    descricao: str,
    valor_abs: float,
    categoria: str,
    conta_bancaria_id: int,
) -> int:
    """Saída em débito/conta: valor armazenado negativo."""
    cat = _categoria_transacao(categoria)
    v = -abs(float(valor_abs))
    conn.execute(
        """
        INSERT INTO transacoes (
          data, descricao, valor, categoria, cartao_id, conta_bancaria_id,
          mes_fatura, realizado
        )
        VALUES (?, ?, ?, ?, NULL, ?, NULL, 1)
        """,
        (data, descricao, v, cat, int(conta_bancaria_id)),
    )
    conn.commit()
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def insert_compra_cartao(
    conn: sqlite3.Connection,
    *,
    data: str,
    descricao: str,
    valor_abs: float,
    categoria: str,
    cartao_id: int,
    mes_fatura: str,
) -> int:
    """Compra no cartão; mês da fatura YYYY-MM; valor negativo."""
    cat = _categoria_transacao(categoria)
    mf = _normalizar_mes_fatura(mes_fatura)
    if mf is None and len(str(data).strip()) >= 7:
        mf = str(data).strip()[:7]
    if mf is None:
        mf = date.today().strftime("%Y-%m")
    v = -abs(float(valor_abs))
    conn.execute(
        """
        INSERT INTO transacoes (
          data, descricao, valor, categoria, cartao_id, conta_bancaria_id,
          mes_fatura, realizado
        )
        VALUES (?, ?, ?, ?, ?, NULL, ?, 1)
        """,
        (data, descricao, v, cat, int(cartao_id), mf),
    )
    conn.commit()
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def total_saidas_debito_mes(conn: sqlite3.Connection, ym: str) -> float:
    row = conn.execute(
        """
        SELECT COALESCE(SUM(ABS(valor)), 0)
        FROM transacoes
        WHERE valor < 0
          AND realizado = 1
          AND conta_bancaria_id IS NOT NULL
          AND strftime('%Y-%m', data) = ?
        """,
        (ym,),
    ).fetchone()
    return float(row[0])


def total_saidas_cartao_mes(conn: sqlite3.Connection, ym: str) -> float:
    row = conn.execute(
        """
        SELECT COALESCE(SUM(ABS(valor)), 0)
        FROM transacoes
        WHERE valor < 0
          AND realizado = 1
          AND cartao_id IS NOT NULL
          AND (
            mes_fatura = ?
            OR (mes_fatura IS NULL AND strftime('%Y-%m', data) = ?)
          )
        """,
        (ym, ym),
    ).fetchone()
    return float(row[0])


def total_saidas_cartao_por_id_mes(
    conn: sqlite3.Connection,
    cartao_id: int,
    ym: str,
) -> float:
    row = conn.execute(
        """
        SELECT COALESCE(SUM(ABS(valor)), 0)
        FROM transacoes
        WHERE valor < 0
          AND realizado = 1
          AND cartao_id = ?
          AND (
            mes_fatura = ?
            OR (mes_fatura IS NULL AND strftime('%Y-%m', data) = ?)
          )
        """,
        (int(cartao_id), ym, ym),
    ).fetchone()
    return float(row[0])


def categorias_saidas_mes(conn: sqlite3.Connection, ym: str) -> list[tuple[str, float]]:
    """Soma ABS(valor) por categoria para saídas (débito + cartão) no período."""
    rows = conn.execute(
        """
        SELECT categoria, COALESCE(SUM(ABS(valor)), 0) AS t
        FROM transacoes
        WHERE valor < 0
          AND realizado = 1
          AND (
            (conta_bancaria_id IS NOT NULL AND strftime('%Y-%m', data) = ?)
            OR (
              cartao_id IS NOT NULL
              AND (
                mes_fatura = ?
                OR (mes_fatura IS NULL AND strftime('%Y-%m', data) = ?)
              )
            )
          )
        GROUP BY categoria
        ORDER BY t DESC
        """,
        (ym, ym, ym),
    ).fetchall()
    return [(str(r[0]), float(r[1])) for r in rows]


def total_parcelas_dividas_ativas(conn: sqlite3.Connection) -> float:
    row = conn.execute(
        """
        SELECT COALESCE(SUM(valor_parcela), 0)
        FROM dividas_emprestimos
        WHERE ativo = 1
        """
    ).fetchone()
    return float(row[0])


def insert_or_update_fatura_fechamento(
    conn: sqlite3.Connection,
    *,
    cartao_id: int,
    mes_referencia_iso: str,
    data_vencimento_iso: str,
    valor_total: float,
) -> int:
    """
    Fechamento mensal (Notion). mes_referencia: primeiro dia do mês (YYYY-MM-DD).
    Não altera registro já marcado como pago.
    """
    mes_ref = str(mes_referencia_iso).strip()[:10]
    dv = str(data_vencimento_iso).strip()[:10]
    ex = conn.execute(
        """
        SELECT id, status_pago FROM faturas_pagas
        WHERE cartao_id = ? AND mes_referencia = ?
        """,
        (int(cartao_id), mes_ref),
    ).fetchone()
    if ex and int(ex["status_pago"]):
        raise ValueError("Esta fatura já está paga; não é possível alterar o fechamento.")
    if ex:
        conn.execute(
            """
            UPDATE faturas_pagas
            SET data_vencimento = ?, valor_total = ?
            WHERE id = ?
            """,
            (dv, float(valor_total), int(ex["id"])),
        )
        conn.commit()
        return int(ex["id"])
    conn.execute(
        """
        INSERT INTO faturas_pagas (cartao_id, mes_referencia, data_vencimento, valor_total, status_pago)
        VALUES (?, ?, ?, ?, 0)
        """,
        (int(cartao_id), mes_ref, dv, float(valor_total)),
    )
    conn.commit()
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def faturas_pendentes(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
          f.id,
          f.cartao_id,
          f.mes_referencia,
          f.data_vencimento,
          f.valor_total,
          c.nome AS cartao_nome
        FROM faturas_pagas f
        JOIN cartoes_credito c ON c.id = f.cartao_id
        WHERE f.status_pago = 0
        ORDER BY f.data_vencimento, f.id
        """
    ).fetchall()


def pagar_fatura(
    conn: sqlite3.Connection,
    fatura_id: int,
    conta_bancaria_id: int,
) -> None:
    """Marca a fatura como paga e gera saída em `transacoes` (débito na conta)."""
    row = conn.execute(
        """
        SELECT f.id, f.mes_referencia, f.valor_total, c.nome
        FROM faturas_pagas f
        JOIN cartoes_credito c ON c.id = f.cartao_id
        WHERE f.id = ? AND f.status_pago = 0
        """,
        (int(fatura_id),),
    ).fetchone()
    if not row:
        raise ValueError("Fatura já paga ou inexistente.")
    conn.execute(
        "UPDATE faturas_pagas SET status_pago = 1 WHERE id = ?",
        (int(fatura_id),),
    )
    mes_lbl = str(row["mes_referencia"])[:7]
    nome = str(row["nome"])
    desc = f"Pagamento Cartão {nome} - {mes_lbl}"
    insert_despesa_debito(
        conn,
        data=date.today().isoformat(),
        descricao=desc,
        valor_abs=float(row["valor_total"]),
        categoria="Cartão",
        conta_bancaria_id=int(conta_bancaria_id),
    )


def despesas_faturas_nao_pagas_por_dia(
    conn: sqlite3.Connection,
    d0: date,
    d1: date,
) -> dict[date, float]:
    """Soma valor_total por dia de vencimento (apenas faturas não pagas)."""
    from collections import defaultdict

    out: defaultdict[date, float] = defaultdict(float)
    for row in conn.execute(
        """
        SELECT data_vencimento, valor_total
        FROM faturas_pagas
        WHERE status_pago = 0
          AND date(data_vencimento) >= date(?)
          AND date(data_vencimento) <= date(?)
        """,
        (d0.isoformat(), d1.isoformat()),
    ):
        ds = str(row[0])[:10]
        try:
            dv = date.fromisoformat(ds)
        except ValueError:
            continue
        out[dv] += float(row[1])
    return dict(out)


def receitas_transacoes_por_dia(
    conn,
    d0: date,
    d1: date,
) -> dict[date, float]:
    """Entradas reais em transacoes (valor > 0, realizado=1) por dia.

    Inclui honorários lançados manualmente e quaisquer outros créditos diretos.
    """
    from collections import defaultdict

    out: defaultdict[date, float] = defaultdict(float)
    for row in conn.execute(
        """
        SELECT data, COALESCE(SUM(valor), 0) AS t
        FROM transacoes
        WHERE valor > 0
          AND realizado = 1
          AND date(data) >= date(?)
          AND date(data) <= date(?)
        GROUP BY data
        """,
        (d0.isoformat(), d1.isoformat()),
    ):
        ds = str(row[0])[:10]
        try:
            dv = date.fromisoformat(ds)
        except ValueError:
            continue
        out[dv] += float(row[1])
    return dict(out)


def despesas_debito_real_por_dia(
    conn: sqlite3.Connection,
    d0: date,
    d1: date,
) -> dict[date, float]:
    """Saídas em conta (débito confirmado), por dia."""
    from collections import defaultdict

    out: defaultdict[date, float] = defaultdict(float)
    for row in conn.execute(
        """
        SELECT data, COALESCE(SUM(ABS(valor)), 0) AS t
        FROM transacoes
        WHERE valor < 0
          AND realizado = 1
          AND conta_bancaria_id IS NOT NULL
          AND cartao_id IS NULL
          AND date(data) >= date(?)
          AND date(data) <= date(?)
        GROUP BY data
        """,
        (d0.isoformat(), d1.isoformat()),
    ):
        ds = str(row[0])[:10]
        try:
            dv = date.fromisoformat(ds)
        except ValueError:
            continue
        out[dv] += float(row[1])
    return dict(out)


def despesas_provisionadas_por_dia(
    conn: sqlite3.Connection,
    d0: date,
    d1: date,
) -> dict[date, float]:
    """Soma valor_previsto por data_prevista (apenas não realizadas)."""
    from collections import defaultdict

    out: defaultdict[date, float] = defaultdict(float)
    for row in conn.execute(
        """
        SELECT data_prevista, COALESCE(SUM(valor_previsto), 0) AS t
        FROM despesas_provisionadas
        WHERE realizado = 0
          AND date(data_prevista) >= date(?)
          AND date(data_prevista) <= date(?)
        GROUP BY data_prevista
        """,
        (d0.isoformat(), d1.isoformat()),
    ):
        ds = str(row[0])[:10]
        try:
            dv = date.fromisoformat(ds)
        except ValueError:
            continue
        out[dv] += float(row[1])
    return dict(out)


def total_provisoes_mes(conn: sqlite3.Connection, ym: str) -> float:
    """Soma de valor_previsto de provisões ainda não realizadas no mês ym (YYYY-MM)."""
    row = conn.execute(
        """
        SELECT COALESCE(SUM(valor_previsto), 0)
        FROM despesas_provisionadas
        WHERE realizado = 0
          AND strftime('%Y-%m', data_prevista) = ?
        """,
        (ym,),
    ).fetchone()
    return float(row[0])


def insert_pagamento_divida(
    conn: sqlite3.Connection,
    *,
    divida_id: int,
    data_pagamento_iso: str,
    valor: float,
    observacao: str | None = None,
    commit: bool = True,
) -> int:
    conn.execute(
        """
        INSERT INTO pagamentos_dividas (divida_id, data_pagamento, valor, observacao)
        VALUES (?, ?, ?, ?)
        """,
        (
            int(divida_id),
            str(data_pagamento_iso).strip()[:10],
            float(valor),
            observacao,
        ),
    )
    rid = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    if commit:
        conn.commit()
    return rid


def insert_divida_emprestimo(
    conn: sqlite3.Connection,
    *,
    instituicao: str,
    descricao: str,
    valor_total: float,
    taxa_juros_mensal_pct: float | None,
    valor_parcela: float,
    parcelas_total: int,
    tipo: str = "Empréstimo",
) -> int:
    """Novo contrato em `dividas_emprestimos` (id autoincrement)."""
    inst = str(instituicao).strip()
    desc = str(descricao).strip()
    if not inst or not desc:
        raise ValueError("Instituição e descrição são obrigatórios.")
    if float(valor_total) <= 0 or float(valor_parcela) <= 0:
        raise ValueError("Valor total e valor da parcela devem ser positivos.")
    nparc = int(parcelas_total)
    if nparc < 1:
        raise ValueError("Total de parcelas deve ser ≥ 1.")
    row_m = conn.execute(
        "SELECT COALESCE(MAX(prioridade), 0) AS m FROM dividas_emprestimos"
    ).fetchone()
    prox_pri = int(row_m[0]) + 1
    taxa = float(taxa_juros_mensal_pct) if taxa_juros_mensal_pct is not None else None
    conn.execute(
        """
        INSERT INTO dividas_emprestimos (
          tipo, instituicao, descricao, valor_total, taxa_juros_mensal_pct,
          taxa_implicita, valor_parcela, parcelas_restantes, saldo_quitacao,
          prioridade, ativo, termino_previsto, observacoes
        )
        VALUES (?, ?, ?, ?, ?, 0, ?, ?, NULL, ?, 1, NULL, NULL)
        """,
        (
            str(tipo).strip()[:80] or "Empréstimo",
            inst[:200],
            desc[:500],
            float(valor_total),
            taxa,
            float(valor_parcela),
            nparc,
            prox_pri,
        ),
    )
    conn.commit()
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def registrar_parcela_divida(
    conn: sqlite3.Connection,
    divida_id: int,
    *,
    data_pagamento_iso: str,
) -> float:
    """
    Registra uma parcela (abatimento patrimonial via `pagamentos_dividas`), sem `transacoes`.
    Usa min(valor_parcela, saldo remanescente). Decrementa `parcelas_restantes` se houver.
    """
    row = conn.execute(
        """
        SELECT id, valor_total, valor_parcela, parcelas_restantes, ativo
        FROM dividas_emprestimos WHERE id = ?
        """,
        (int(divida_id),),
    ).fetchone()
    if not row or not int(row["ativo"]):
        raise ValueError("Dívida inexistente ou inativa.")
    ini = float(row["valor_total"])
    vp = float(row["valor_parcela"])
    tp = total_pagamentos_divida(conn, int(divida_id))
    rem = max(0.0, ini - tp)
    if rem <= 0:
        raise ValueError("Saldo devedor já zerado para este contrato.")
    valor_lanc = min(vp, rem)
    obs = "Parcela (controle patrimonial)"
    conn.execute(
        """
        INSERT INTO pagamentos_dividas (divida_id, data_pagamento, valor, observacao)
        VALUES (?, ?, ?, ?)
        """,
        (
            int(divida_id),
            str(data_pagamento_iso).strip()[:10],
            float(valor_lanc),
            obs,
        ),
    )
    pr = row["parcelas_restantes"]
    if pr is not None and int(pr) > 0:
        conn.execute(
            """
            UPDATE dividas_emprestimos
            SET parcelas_restantes = parcelas_restantes - 1
            WHERE id = ?
            """,
            (int(divida_id),),
        )
    conn.commit()
    return float(valor_lanc)


def total_pagamentos_divida(conn: sqlite3.Connection, divida_id: int) -> float:
    row = conn.execute(
        """
        SELECT COALESCE(SUM(valor), 0) FROM pagamentos_dividas WHERE divida_id = ?
        """,
        (int(divida_id),),
    ).fetchone()
    return float(row[0])


def _data_mais_n_meses(d: date, delta_meses: int) -> date:
    """Avança `delta_meses` a partir de `d`, mantendo o dia quando possível (ajuste ao fim do mês)."""
    y, m = d.year, d.month + int(delta_meses)
    while m > 12:
        y += 1
        m -= 12
    while m < 1:
        y -= 1
        m += 12
    ult = calendar.monthrange(y, m)[1]
    dia = min(d.day, ult)
    return date(y, m, dia)


def insert_despesa_provisionada(
    conn: sqlite3.Connection,
    *,
    data_prevista_iso: str,
    descricao: str,
    valor_previsto: float,
    categoria: str,
) -> int:
    conn.execute(
        """
        INSERT INTO despesas_provisionadas (
          data_prevista, descricao, valor_previsto, categoria, realizado
        )
        VALUES (?, ?, ?, ?, 0)
        """,
        (
            str(data_prevista_iso).strip()[:10],
            str(descricao).strip(),
            float(valor_previsto),
            _categoria_transacao(categoria),
        ),
    )
    conn.commit()
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def insert_provisoes_recorrentes(
    conn: sqlite3.Connection,
    *,
    data_prevista_inicial_iso: str,
    descricao: str,
    valor_previsto: float,
    categoria: str,
    recorrencia_mensal: bool,
    quantidade_meses: int,
) -> int:
    """
    Única: 1 linha (descrição sem prefixo). Mensal: N linhas com `[REC] ` + mesma descrição base,
    datas em sequência mês a mês a partir da data inicial. Retorna quantidade inserida.
    """
    base = str(descricao).strip()
    if not base:
        raise ValueError("Informe a descrição.")
    if float(valor_previsto) <= 0:
        raise ValueError("Valor previsto deve ser positivo.")
    cat = _categoria_transacao(categoria)
    d0 = date.fromisoformat(str(data_prevista_inicial_iso).strip()[:10])

    if not recorrencia_mensal:
        conn.execute(
            """
            INSERT INTO despesas_provisionadas (
              data_prevista, descricao, valor_previsto, categoria, realizado
            )
            VALUES (?, ?, ?, ?, 0)
            """,
            (d0.isoformat(), base, float(valor_previsto), cat),
        )
        conn.commit()
        return 1

    n = max(1, min(120, int(quantidade_meses)))
    for k in range(n):
        dk = _data_mais_n_meses(d0, k)
        desc_linha = f"[REC] {base}"
        conn.execute(
            """
            INSERT INTO despesas_provisionadas (
              data_prevista, descricao, valor_previsto, categoria, realizado
            )
            VALUES (?, ?, ?, ?, 0)
            """,
            (dk.isoformat(), desc_linha, float(valor_previsto), cat),
        )
    conn.commit()
    return n


def entradas_extras_realizadas_por_dia_intervalo(
    conn: sqlite3.Connection,
    d0: date,
    d1: date,
) -> dict[date, float]:
    from collections import defaultdict

    out: defaultdict[date, float] = defaultdict(float)
    for row in conn.execute(
        """
        SELECT data, COALESCE(SUM(valor), 0) AS t
        FROM entradas_extras
        WHERE COALESCE(status, 'Realizado') = 'Realizado'
          AND date(data) >= date(?)
          AND date(data) <= date(?)
        GROUP BY data
        ORDER BY data ASC
        """,
        (d0.isoformat(), d1.isoformat()),
    ):
        ds = str(row[0])[:10]
        try:
            dv = date.fromisoformat(ds)
        except ValueError:
            continue
        out[dv] += float(row[1])
    return dict(out)


def entradas_extras_provisionadas_por_dia_intervalo(
    conn: sqlite3.Connection,
    d0: date,
    d1: date,
) -> dict[date, float]:
    from collections import defaultdict

    out: defaultdict[date, float] = defaultdict(float)
    for row in conn.execute(
        """
        SELECT data, COALESCE(SUM(valor), 0) AS t
        FROM entradas_extras
        WHERE COALESCE(status, 'Realizado') = 'Provisionado'
          AND date(data) >= date(?)
          AND date(data) <= date(?)
        GROUP BY data
        ORDER BY data ASC
        """,
        (d0.isoformat(), d1.isoformat()),
    ):
        ds = str(row[0])[:10]
        try:
            dv = date.fromisoformat(ds)
        except ValueError:
            continue
        out[dv] += float(row[1])
    return dict(out)


def insert_entradas_extras_recorrentes(
    conn: sqlite3.Connection,
    *,
    data_prevista_inicial_iso: str,
    descricao: str,
    valor: float,
    categoria: str,
    origem: str | None,
    status: str,
    recorrencia_mensal: bool,
    quantidade_meses: int,
) -> int:
    """
    Única: 1 linha em `entradas_extras`. Mensal: N linhas com `[REC] ` na descrição (exceto se status
    Realizado e você quiser rastreio — aqui [REC] só em recorrência mensal).
    """
    st = str(status).strip()
    if st not in ("Realizado", "Provisionado"):
        st = "Realizado"
    base = str(descricao).strip()
    if not base:
        raise ValueError("Informe a descrição.")
    if float(valor) <= 0:
        raise ValueError("Valor deve ser positivo.")
    cat = _categoria_transacao(categoria)
    d0 = date.fromisoformat(str(data_prevista_inicial_iso).strip()[:10])

    if not recorrencia_mensal:
        conn.execute(
            """
            INSERT INTO entradas_extras (data, descricao, valor, categoria, origem, status)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (d0.isoformat(), base, float(valor), cat, origem, st),
        )
        conn.commit()
        return 1

    n = max(1, min(120, int(quantidade_meses)))
    for k in range(n):
        dk = _data_mais_n_meses(d0, k)
        desc_linha = f"[REC] {base}"
        conn.execute(
            """
            INSERT INTO entradas_extras (data, descricao, valor, categoria, origem, status)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (dk.isoformat(), desc_linha, float(valor), cat, origem, st),
        )
    conn.commit()
    return n


def realizar_despesa_provisionada(
    conn: sqlite3.Connection,
    prov_id: int,
    *,
    data_real_iso: str,
    valor_real: float,
    conta_bancaria_id: int,
) -> int:
    row = conn.execute(
        """
        SELECT id, descricao, categoria, realizado
        FROM despesas_provisionadas
        WHERE id = ?
        """,
        (int(prov_id),),
    ).fetchone()
    if not row or int(row["realizado"]):
        raise ValueError("Provisão inexistente ou já realizada.")
    desc = str(row["descricao"])
    cat = str(row["categoria"])
    tid = insert_despesa_debito(
        conn,
        data=str(data_real_iso).strip()[:10],
        descricao=desc,
        valor_abs=float(valor_real),
        categoria=cat,
        conta_bancaria_id=int(conta_bancaria_id),
    )
    conn.execute(
        """
        UPDATE despesas_provisionadas
        SET realizado = 1,
            transacao_id = ?,
            data_realizada = ?,
            valor_real = ?,
            conta_bancaria_id = ?
        WHERE id = ?
        """,
        (
            int(tid),
            str(data_real_iso).strip()[:10],
            float(valor_real),
            int(conta_bancaria_id),
            int(prov_id),
        ),
    )
    conn.commit()
    return int(tid)
