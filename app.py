"""Painel Streamlit — CFO Pessoal / Auditax (SQLite local)."""

from __future__ import annotations

import calendar
import math
import os
import re
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

import database as db

APP_DIR = Path(__file__).resolve().parent
DATABASE_FILE = (APP_DIR / "database.db").resolve()
# Conexão gerenciada por database.py → PgConn (psycopg2) ou SQLite local (fallback).

# Resumo de faturas: rótulo exibido + nome exato em `cartoes_credito` (import/csv/cartoes_credito.csv).
CARTOES_RESUMO: list[tuple[str, str]] = [
    ("Itaú Azul", "Cartão Itaú Azul"),
    ("Itaú Black", "Cartão Itaú Black"),
    ("BRB (Nação)", "Cartão Nação BRB"),
    ("Inter", "Cartão Bco Inter"),
    ("Porto", "Cartão Porto Bank"),
]


def _cartao_id_por_nome(conn: sqlite3.Connection, nome: str) -> int | None:
    row = conn.execute(
        "SELECT id FROM cartoes_credito WHERE nome = ?",
        (nome,),
    ).fetchone()
    return int(row[0]) if row else None

CATEGORIAS_ENTRADAS_EXTRAS = (
    "Aporte",
    "Resgate",
    "Empréstimo",
    "Receita Eventual",
    "Receita Imposto de Renda",
    "Outros",
)
EE_STATUS_EXTRAS = ("Realizado", "Provisionado")

# Entradas extras que compõem indicador de faturamento/performance no mês (além dos honorários).
EE_CATEGORIAS_FATURAMENTO_MES: tuple[str, ...] = (
    "Receita Eventual",
    "Receita Imposto de Renda",
)

# Movimentação patrimonial: entra no saldo de caixa, mas não no faturamento de performance.
EE_CATEGORIAS_MOV_CAPITAL: tuple[str, ...] = (
    "Aporte",
    "Resgate",
    "Empréstimo",
    "Outros",
)

# Despesas em débito e compras nos cartões (lançamento manual).
CATEGORIAS_DESPESA_DEBITO = (
    "Gasolina",
    "Alimentação",
    "Assinaturas",
    "Aluguel",
    "Outros",
    "Dívida / parcela",
)

# Azul claro — entradas que não são receita de serviço (Auditax).
COR_ENTRADAS_EXTRAS = "#93c5fd"

COLUNAS_ESPERADAS_DIVIDAS = (
    "id, tipo, instituicao, descricao, valor_total, taxa_juros_mensal_pct, "
    "taxa_implicita, valor_parcela, parcelas_restantes, saldo_quitacao, "
    "prioridade, ativo, termino_previsto, observacoes"
)

COLUNAS_ESPERADAS_CARTOES = (
    "id, nome, limite, melhor_dia_compra, dia_vencimento, created_at"
)

def brl(x: float) -> str:
    s = f"{x:,.2f}"
    return "R$ " + s.replace(",", "v").replace(".", ",").replace("v", ".")


def _ym_ref(d: date) -> str:
    return f"{d.year}-{d.month:02d}"


def _meses_colunas(d0: date, n: int) -> list[str]:
    """Datas do 1º dia de cada mês (ISO) para chave `mes_referencia` em `faturas_pagas`."""
    out: list[str] = []
    d = date(d0.year, d0.month, 1)
    for _ in range(int(n)):
        out.append(d.isoformat())
        d = _add_one_month(d)
    return out


def fig_pie_saidas_tres_grupos(debito: float, provisoes: float, cartoes: float) -> go.Figure:
    """Pizza: saídas em 3 grupos (débito, provisões do mês, faturas de cartão)."""
    labels = ["Despesas (débito)", "Provisões (mês)", "Cartões (faturas)"]
    values = [max(0.0, float(debito)), max(0.0, float(provisoes)), max(0.0, float(cartoes))]
    total = sum(values)
    if total <= 0:
        fig = go.Figure()
        fig.add_annotation(
            text="Sem saídas registradas neste recorte",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(size=14),
        )
        fig.update_layout(template="plotly_white", height=380)
        return fig
    fig = go.Figure(
        data=[
            go.Pie(
                labels=labels,
                values=values,
                hole=0.35,
                marker=dict(colors=["#1d4ed8", "#b45309", "#059669"]),
                textinfo="label+percent",
            )
        ]
    )
    fig.update_layout(
        title="Saídas por origem",
        template="plotly_white",
        height=400,
        margin=dict(t=50, b=30, l=30, r=30),
    )
    return fig


def fig_pie_categorias_saidas(pairs: list[tuple[str, float]]) -> go.Figure:
    """Pizza: distribuição por categoria (débito + cartão no mês)."""
    if not pairs or sum(p[1] for p in pairs) <= 0:
        fig = go.Figure()
        fig.add_annotation(
            text="Sem saídas categorizadas no mês",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(size=14),
        )
        fig.update_layout(template="plotly_white", height=380)
        return fig
    labels = [p[0] for p in pairs]
    values = [p[1] for p in pairs]
    fig = go.Figure(
        data=[
            go.Pie(
                labels=labels,
                values=values,
                hole=0.3,
                textinfo="label+percent",
            )
        ]
    )
    fig.update_layout(
        title="Onde o dinheiro está indo (categoria)",
        template="plotly_white",
        height=400,
        margin=dict(t=50, b=30, l=30, r=30),
    )
    return fig


def _entradas_extras_por_dia(conn: sqlite3.Connection) -> defaultdict[date, float]:
    """Apenas entradas **Realizado** (compat: uso legado no fluxo)."""
    out: defaultdict[date, float] = defaultdict(float)
    for row in conn.execute(
        """
        SELECT data, COALESCE(SUM(valor), 0) FROM entradas_extras
        WHERE COALESCE(status, 'Realizado') = 'Realizado'
        GROUP BY data
        """
    ):
        ds = str(row[0])[:10]
        try:
            d = date.fromisoformat(ds)
        except ValueError:
            continue
        out[d] += float(row[1])
    return out


def _entradas_extras_por_mes(conn: sqlite3.Connection) -> dict[str, float]:
    return {
        str(r[0]): float(r[1])
        for r in conn.execute(
            """
            SELECT strftime('%Y-%m', data) AS ym, COALESCE(SUM(valor), 0)
            FROM entradas_extras
            WHERE COALESCE(status, 'Realizado') = 'Realizado'
            GROUP BY ym
            """
        )
        if r[0]
    }


def _sync_entradas_extras_editor(
    conn: sqlite3.Connection,
    baseline: pd.DataFrame,
    edited: pd.DataFrame,
) -> None:
    """Persiste inserções, atualizações e exclusões a partir do data_editor."""
    if edited.empty and baseline.empty:
        return
    base_ids: set[int] = set()
    if not baseline.empty and "id" in baseline.columns:
        for v in baseline["id"].dropna():
            try:
                base_ids.add(int(float(v)))
            except (TypeError, ValueError):
                pass
    edited_ids: set[int] = set()
    for v in edited.get("id", pd.Series(dtype=object)).dropna():
        try:
            edited_ids.add(int(float(v)))
        except (TypeError, ValueError):
            pass
    for oid in base_ids - edited_ids:
        db.delete_entrada_extra(conn, oid)

    for _, row in edited.iterrows():
        rid = row.get("id")
        ds = row.get("data")
        if hasattr(ds, "isoformat"):
            data_s = ds.isoformat()[:10]
        else:
            data_s = str(ds)[:10]
        desc = str(row.get("descricao", "")).strip()
        if not desc:
            continue
        try:
            val = float(row["valor"])
        except (TypeError, ValueError):
            continue
        if val <= 0:
            continue
        cat = str(row.get("categoria", "Outros")).strip()
        if cat not in CATEGORIAS_ENTRADAS_EXTRAS:
            cat = "Outros"
        orig_cell = row.get("origem")
        orig = None if pd.isna(orig_cell) or orig_cell is None else str(orig_cell).strip()
        if orig == "":
            orig = None
        raw_st = row.get("status", "Realizado")
        if pd.isna(raw_st) or str(raw_st).strip() == "":
            stx = "Realizado"
        else:
            stx = str(raw_st).strip()
        if stx not in EE_STATUS_EXTRAS:
            stx = "Realizado"

        is_new = pd.isna(rid) or rid is None or str(rid).strip() == ""
        if is_new:
            db.insert_entrada_extra(
                conn,
                data=data_s,
                descricao=desc,
                valor=val,
                categoria=cat,
                origem=orig,
                status=stx,
            )
            continue
        try:
            iid = int(float(rid))
        except (TypeError, ValueError):
            db.insert_entrada_extra(
                conn,
                data=data_s,
                descricao=desc,
                valor=val,
                categoria=cat,
                origem=orig,
                status=stx,
            )
            continue
        if iid in base_ids:
            db.update_entrada_extra(
                conn,
                iid,
                data=data_s,
                descricao=desc,
                valor=val,
                categoria=cat,
                origem=orig,
                status=stx,
            )
        else:
            db.insert_entrada_extra(
                conn,
                data=data_s,
                descricao=desc,
                valor=val,
                categoria=cat,
                origem=orig,
                status=stx,
            )


def _hint_sql_erro(exc: sqlite3.OperationalError) -> str:
    msg = str(exc)
    low = msg.lower()
    if "no such column" in low:
        col = re.search(r"no such column:\s*(\S+)", low)
        nome = col.group(1) if col else "desconhecida"
        return (
            f"**Coluna ausente no banco:** `{nome}`.\n\n"
            f"O schema atual não bate com o esperado. **Colunas esperadas em "
            f"`dividas_emprestimos`:** {COLUNAS_ESPERADAS_DIVIDAS}.\n\n"
            "**Correção:** apague `database.db` e rode `python3 seed_db.py`."
        )
    if "no such table" in low:
        tab = re.search(r"no such table:\s*(\S+)", low)
        nome = tab.group(1) if tab else "desconhecida"
        return (
            f"**Tabela ausente:** `{nome}`. Apague `database.db` e rode "
            "`python3 seed_db.py`."
        )
    return f"`{msg}`"


def _taxa_aa_nominal_pct(mensal: float | None, taxa_implicita: int) -> float | None:
    """a.a. nominal a partir da % a.m. do CSV (ex.: 18.33% a.m. → ~220% a.a.)."""
    if taxa_implicita == 1 or mensal is None:
        return None
    try:
        m = float(mensal)
    except (TypeError, ValueError):
        return None
    if math.isnan(m):
        return None
    return ((1.0 + m / 100.0) ** 12 - 1.0) * 100.0


def _divida_progresso_e_cet_md(
    row: pd.Series,
    total_pago: float,
    n_lancamentos: int,
) -> tuple[float, str]:
    """(progresso 0–1, texto markdown CET / custo simplificado)."""
    vt = float(row["valor_total"])
    pago = max(0.0, float(total_pago))
    prog = min(1.0, pago / vt) if vt > 0 else 0.0

    parts: list[str] = []
    vp = float(row["valor_parcela"])
    pr_raw = row.get("parcelas_restantes")
    n_tot: int | None = None
    if pr_raw is not None and not pd.isna(pr_raw):
        try:
            n_tot = int(pr_raw) + int(n_lancamentos)
        except (TypeError, ValueError):
            n_tot = None
    if n_tot is not None and n_tot > 0 and vt > 0:
        custo_fin = max(0.0, vp * float(n_tot) - vt)
        pct_c = (custo_fin / vt) * 100.0 if vt else 0.0
        parts.append(
            f"**Custo efetivo total (simplificado)** — estimativa juros: parcela × **{n_tot}** parcelas "
            f"menos principal = **{brl(custo_fin)}** (~**{pct_c:.1f}%** do principal). "
            f"_Referência interna; não substitui CET regulatório._"
        )
    tx = row.get("taxa_juros_mensal_pct")
    txi = int(float(row.get("taxa_implicita") or 0))
    if txi != 1 and tx is not None and not pd.isna(tx):
        try:
            rm = float(tx)
        except (TypeError, ValueError):
            rm = None
        else:
            faa = _taxa_aa_nominal_pct(rm, 0)
            if faa is not None:
                parts.append(
                    f"**Taxa informada:** {rm:.4f}% **ao mês** → fator **anual equivalente** "
                    f"(composto) ~**{faa:.2f}% ao ano**."
                )
    texto = "\n\n".join(parts) if parts else ""
    return prog, texto


def _last_day_of_month(y: int, m: int) -> int:
    return calendar.monthrange(y, m)[1]


def _safe_dom(dia: int, y: int, m: int) -> int:
    """Dia do mês ajustado ao último dia (ex.: venc. 31 em abril → 30)."""
    return min(int(dia), _last_day_of_month(y, m))


def _valor_mensal_cartao(conn: sqlite3.Connection, cartao_id: int) -> float:
    """Estimativa de fatura mensal do cartão a partir de `transacoes` (saídas negativas)."""
    row = conn.execute(
        """
        SELECT SUM(CASE WHEN valor < 0 THEN valor ELSE 0 END) AS saidas_neg,
               COUNT(DISTINCT strftime('%Y-%m', data)) AS meses
        FROM transacoes
        WHERE cartao_id = ?
          AND realizado = 1
        """,
        (int(cartao_id),),
    ).fetchone()
    if not row or row[0] is None:
        return 0.0
    saida_abs = -float(row[0] or 0)
    meses = max(int(row[1] or 1), 1)
    return saida_abs / meses


def _add_one_month(d: date) -> date:
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)


def _sub_months(d: date, n: int) -> date:
    """Primeiro dia do mês, n meses antes de d (d deve ser dia 1)."""
    y, m = d.year, d.month
    m -= n
    while m <= 0:
        m += 12
        y -= 1
    return date(y, m, 1)


def _honorarios_mes_teorico(conn: sqlite3.Connection, y: int, m: int) -> float:
    """Honorários mensais teóricos (carteira ativa + vigência)."""
    ult = date(y, m, _last_day_of_month(y, m)).isoformat()
    row = conn.execute(
        """
        SELECT COALESCE(SUM(valor_honorario), 0)
        FROM clientes
        WHERE status = 'Ativo'
          AND (
            honorario_vigencia_inicio IS NULL
            OR honorario_vigencia_inicio <= ?
          )
        """,
        (ult,),
    ).fetchone()
    return float(row[0])


def _faturas_cartoes_mensal_estimada(conn: sqlite3.Connection) -> float:
    """Soma das estimativas mensais dos 5 cartões-chave."""
    total = 0.0
    for _label, nome_cartao in CARTOES_RESUMO:
        cid = _cartao_id_por_nome(conn, nome_cartao)
        if cid is None:
            continue
        total += _valor_mensal_cartao(conn, cid)
    return total


def _previsto_despesas_mensal(conn: sqlite3.Connection, ym: str) -> float:
    """Previsto mensal: faturas cartões (estimativa) + provisões não realizadas no mês."""
    try:
        prov = db.total_provisoes_mes(conn, ym)
    except sqlite3.OperationalError:
        prov = 0.0
    return _faturas_cartoes_mensal_estimada(conn) + prov


def build_realizado_previsto_df(
    conn: sqlite3.Connection,
    mes_inicio: date,
    n_meses: int,
) -> pd.DataFrame:
    """
    Realizado: entradas/saídas reais em `transacoes` por mês.
    Previsto: honorários teóricos + faturas cartões (estimativa) + provisões do mês.
    """
    real = db.read_sql(
        """
        SELECT
          strftime('%Y-%m', data) AS ym,
          SUM(CASE WHEN valor > 0 THEN valor ELSE 0 END) AS entradas,
          SUM(CASE WHEN valor < 0 THEN -valor ELSE 0 END) AS saidas
        FROM transacoes
        WHERE realizado = 1
        GROUP BY ym
        ORDER BY ym
        """,
        conn,
    )
    por_mes: dict[str, tuple[float, float]] = {}
    for _, row in real.iterrows():
        por_mes[str(row["ym"])] = (float(row["entradas"]), float(row["saidas"]))

    extras_mes = _entradas_extras_por_mes(conn)

    rows: list[dict[str, object]] = []
    d = date(mes_inicio.year, mes_inicio.month, 1)
    for _ in range(n_meses):
        y, m = d.year, d.month
        ym = f"{y}-{m:02d}"
        rr_svc, rd = por_mes.get(ym, (0.0, 0.0))
        rr_ext = float(extras_mes.get(ym, 0.0))
        rr_total = rr_svc + rr_ext
        pr = _honorarios_mes_teorico(conn, y, m)
        pdesp = _previsto_despesas_mensal(conn, ym)
        rows.append(
            {
                "Mês": f"{m:02d}/{y}",
                "ym": ym,
                "Receitas (serviço)": rr_svc,
                "Entradas extras": rr_ext,
                "Receitas (real)": rr_total,
                "Receitas (previsto)": pr,
                "Despesas (real)": rd,
                "Despesas (previsto)": pdesp,
                "Var. receitas": rr_svc - pr,
                "Var. despesas": rd - pdesp,
            }
        )
        d = _add_one_month(d)

    return pd.DataFrame(rows)


def fig_realizado_previsto(df: pd.DataFrame) -> go.Figure:
    """Receitas: serviço (transações), entradas extras (azul), previsto (honorários). Despesas: real vs previsto."""
    x = df["Mês"].tolist()
    fig = make_subplots(
        rows=2,
        cols=1,
        subplot_titles=("Receitas", "Despesas"),
        vertical_spacing=0.14,
    )
    fig.add_trace(
        go.Bar(
            name="Receitas (serviço)",
            x=x,
            y=df["Receitas (serviço)"],
            marker_color="#15803d",
            legendgroup="g1",
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Bar(
            name="Entradas extras",
            x=x,
            y=df["Entradas extras"],
            marker_color=COR_ENTRADAS_EXTRAS,
            legendgroup="g1",
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Bar(
            name="Previsto (honorários)",
            x=x,
            y=df["Receitas (previsto)"],
            marker_color="#86efac",
            legendgroup="g1",
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Bar(
            name="Realizado",
            x=x,
            y=df["Despesas (real)"],
            marker_color="#b91c1c",
            legendgroup="g2",
            showlegend=False,
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Bar(
            name="Previsto",
            x=x,
            y=df["Despesas (previsto)"],
            marker_color="#fca5a5",
            legendgroup="g2",
            showlegend=False,
        ),
        row=2,
        col=1,
    )
    fig.update_layout(
        barmode="group",
        height=640,
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=80, b=40),
    )
    fig.update_yaxes(title_text="R$", row=1, col=1)
    fig.update_yaxes(title_text="R$", row=2, col=1)
    fig.update_xaxes(tickangle=-35, row=1, col=1)
    fig.update_xaxes(tickangle=-35, row=2, col=1)
    return fig


def _style_realizado_previsto(df: pd.DataFrame):
    """Destaca variações: receitas a mais em verde; despesas a mais em vermelho."""

    def _green(v: object) -> str:
        try:
            if v is not None and not (isinstance(v, float) and math.isnan(v)) and float(v) > 0:
                return "background-color: #d1fae5; color: #065f46"
        except (TypeError, ValueError):
            pass
        return ""

    def _red(v: object) -> str:
        try:
            if v is not None and not (isinstance(v, float) and math.isnan(v)) and float(v) > 0:
                return "background-color: #fecaca; color: #991b1b"
        except (TypeError, ValueError):
            pass
        return ""

    sty = df.style
    if "Var. receitas" in df.columns:
        sty = sty.applymap(_green, subset=["Var. receitas"])
    if "Var. despesas" in df.columns:
        sty = sty.applymap(_red, subset=["Var. despesas"])
    return sty.format(
        {
            "Receitas (serviço)": "R$ {:,.2f}",
            "Entradas extras": "R$ {:,.2f}",
            "Receitas (real)": "R$ {:,.2f}",
            "Receitas (previsto)": "R$ {:,.2f}",
            "Despesas (real)": "R$ {:,.2f}",
            "Despesas (previsto)": "R$ {:,.2f}",
            "Var. receitas": "R$ {:,.2f}",
            "Var. despesas": "R$ {:,.2f}",
        },
        na_rep="—",
    )


def build_fluxo_projetado(
    conn: sqlite3.Connection,
    data_inicio: date,
    n_dias: int = 45,
) -> tuple[pd.DataFrame, float, date | None]:
    """
    Uma linha por dia: honorários no dia_vencimento de cada cliente;
    saídas **reais** em `transacoes` (débito em conta); saídas **previstas** = faturas de cartão
    não pagas (`faturas_pagas`) + despesas **provisionadas** não realizadas.
    Entradas extras **Provisionado** compõem o saldo projetado (aparecem também como série separada no gráfico).
    **Dívidas** (`dividas_emprestimos`) não entram no fluxo — use a aba Despesas para o caixa.
    Saldo inicial = caixa até antes do primeiro dia da janela; transações só entram se **realizado = 1**.
    """
    saldo0 = db.saldo_caixa_ate_data(conn, data_inicio)
    receitas_servico: dict[date, float] = defaultdict(float)
    data_fim = data_inicio + timedelta(days=n_dias - 1)
    try:
        extras_real_dia = db.entradas_extras_realizadas_por_dia_intervalo(
            conn, data_inicio, data_fim
        )
        extras_prov_dia = db.entradas_extras_provisionadas_por_dia_intervalo(
            conn, data_inicio, data_fim
        )
    except sqlite3.OperationalError:
        extras_real_dia = {}
        extras_prov_dia = {}
    faturas_nao_pagas_dia = db.despesas_faturas_nao_pagas_por_dia(
        conn, data_inicio, data_fim
    )
    try:
        debito_real_dia = db.despesas_debito_real_por_dia(conn, data_inicio, data_fim)
    except sqlite3.OperationalError:
        debito_real_dia = {}
    try:
        provisao_dia = db.despesas_provisionadas_por_dia(conn, data_inicio, data_fim)
    except sqlite3.OperationalError:
        provisao_dia = {}

    d = data_inicio
    while d <= data_fim:
        y, m = d.year, d.month

        for _cid, _nome, honor, dia_v, vig in conn.execute(
            """
            SELECT id, nome, valor_honorario, dia_vencimento, honorario_vigencia_inicio
            FROM clientes
            WHERE status = 'Ativo'
            """
        ):
            dom = _safe_dom(int(dia_v), y, m)
            if d.day != dom:
                continue
            if vig is not None and str(vig).strip():
                try:
                    vig_d = date.fromisoformat(str(vig)[:10])
                except ValueError:
                    vig_d = None
                if vig_d is not None and d < vig_d:
                    continue
            receitas_servico[d] += float(honor)

        d += timedelta(days=1)

    rows: list[dict[str, object]] = []
    saldo = saldo0
    d = data_inicio
    while d <= data_fim:
        r_svc = float(receitas_servico.get(d, 0.0))
        r_ext = float(extras_real_dia.get(d, 0.0))
        r_ext_prev = float(extras_prov_dia.get(d, 0.0))
        r = r_svc + r_ext + r_ext_prev
        dr = float(debito_real_dia.get(d, 0.0))
        dp = float(faturas_nao_pagas_dia.get(d, 0.0)) + float(provisao_dia.get(d, 0.0))
        s = dr + dp
        saldo += r - s
        rows.append(
            {
                "data": d,
                "dia": d.strftime("%d/%m/%Y"),
                "dia_curto": d.strftime("%d/%m"),
                "receitas_servico": r_svc,
                "receitas_extras": r_ext,
                "receitas_extras_previstas": r_ext_prev,
                "receitas": r,
                "despesas": s,
                "despesas_real": dr,
                "despesas_previstas": dp,
                "liquido": r - s,
                "saldo_projetado": saldo,
            }
        )
        d += timedelta(days=1)

    df = pd.DataFrame(rows)
    if df.empty:
        return df, saldo0, None
    idx_min = df["saldo_projetado"].astype(float).idxmin()
    data_pior = df.loc[idx_min, "data"]
    if isinstance(data_pior, pd.Timestamp):
        data_pior = data_pior.date()
    return df, saldo0, data_pior


def _proxima_data_dia_mes(ref: date, dia_mes: int) -> date:
    """Próxima data ≥ ref com o dia do mês desejado (ajustado ao fim do mês)."""
    y, m = ref.year, ref.month
    ult = _last_day_of_month(y, m)
    d = min(int(dia_mes), ult)
    cand = date(y, m, d)
    if cand >= ref:
        return cand
    if m == 12:
        y, m = y + 1, 1
    else:
        m += 1
    ult = _last_day_of_month(y, m)
    d = min(int(dia_mes), ult)
    return date(y, m, d)


def upcoming_events_md(conn: sqlite3.Connection, ref: date) -> str:
    """Texto Markdown com próximos vencimentos (honorários, cartões, parcelas)."""
    linhas: list[str] = []
    for nome, d_v, v in conn.execute(
        """
        SELECT nome, dia_vencimento, valor_honorario
        FROM clientes
        WHERE status = 'Ativo'
        ORDER BY dia_vencimento
        LIMIT 8
        """
    ):
        nd = _proxima_data_dia_mes(ref, int(d_v))
        linhas.append(f"**{nd.strftime('%d/%m/%Y')}** — Honorário **{nome}**: {brl(float(v))}")

    for row in conn.execute(
        """
        SELECT nome, dia_vencimento FROM cartoes_credito
        WHERE dia_vencimento IS NOT NULL
        ORDER BY nome COLLATE NOCASE
        """
    ):
        nd = _proxima_data_dia_mes(ref, int(row["dia_vencimento"]))
        nm = row["nome"] or "Cartão"
        linhas.append(f"**{nd.strftime('%d/%m/%Y')}** — Fatura **{nm}**")

    return "\n\n".join(linhas) if linhas else "_Sem vencimentos cadastrados._"


def fig_fluxo_diario(df: pd.DataFrame) -> go.Figure:
    """Barras: receitas; despesas realizadas (vermelho sólido) + previstas (vermelho tracejado) + saldo."""
    x = df["dia"].tolist()
    rec_svc = df["receitas_servico"].astype(float)
    rec_ext = df["receitas_extras"].astype(float)
    if "despesas_real" in df.columns and "despesas_previstas" in df.columns:
        desp_r = df["despesas_real"].astype(float)
        desp_p = df["despesas_previstas"].astype(float)
    else:
        desp_r = df["despesas"].astype(float)
        desp_p = df["despesas"] * 0.0
    saldo = df["saldo_projetado"].astype(float)

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            name="Receitas (serviço)",
            x=x,
            y=rec_svc,
            marker=dict(color="#16a34a", line=dict(width=0)),
            legendgroup="rec",
        )
    )
    fig.add_trace(
        go.Bar(
            name="Entradas extras",
            x=x,
            y=rec_ext,
            marker=dict(color=COR_ENTRADAS_EXTRAS, line=dict(width=0)),
            legendgroup="rec",
        )
    )
    if "receitas_extras_previstas" in df.columns:
        rec_prev = df["receitas_extras_previstas"].astype(float)
        if rec_prev.abs().sum() > 0:
            fig.add_trace(
                go.Scatter(
                    name="Entrada prevista (extras)",
                    x=x,
                    y=rec_prev,
                    mode="lines+markers",
                    line=dict(color="#1d4ed8", width=2, dash="dot"),
                    marker=dict(size=6, symbol="diamond", color="#1d4ed8"),
                    legendgroup="rec_prev",
                )
            )
    fig.add_trace(
        go.Bar(
            name="Despesas (realizadas)",
            x=x,
            y=-desp_r,
            marker=dict(color="#b91c1c", line=dict(width=0)),
            legendgroup="desp",
        )
    )
    fig.add_trace(
        go.Bar(
            name="Despesas (provisionadas / faturas)",
            x=x,
            y=-desp_p,
            marker=dict(
                color="rgba(220, 38, 38, 0.42)",
                line=dict(width=1.2, color="#dc2626"),
                pattern=dict(shape="/", solidity=0.4, fgcolor="#dc2626"),
            ),
            legendgroup="desp",
        )
    )
    fig.add_trace(
        go.Scatter(
            name="Saldo previsto (acum. c/ provisões)",
            x=x,
            y=saldo,
            mode="lines+markers",
            line=dict(color="#7c3aed", width=2.5),
            marker=dict(size=5, color="#7c3aed"),
        )
    )

    fig.update_layout(
        barmode="relative",
        bargap=0.22,
        height=520,
        margin=dict(t=50, b=80, l=60, r=40),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        yaxis=dict(
            title="R$",
            zeroline=True,
            zerolinewidth=1,
            gridcolor="rgba(0,0,0,0.06)",
        ),
        xaxis=dict(
            title="Dia",
            tickangle=-45,
            tickfont=dict(size=10),
            type="category",
        ),
        hovermode="x unified",
        template="plotly_white",
    )
    return fig


def get_conn():
    """
    Retorna conexão ativa: Supabase (PostgreSQL) se secrets.toml configurado,
    caso contrário SQLite local. Fecha a conexão anterior do Streamlit para
    não acumular handles.
    """
    using_pg = db._read_pg_url() is not None
    if not using_pg and db.DB_PATH.resolve() != DATABASE_FILE:
        st.error(
            f"Inconsistência de caminho: `database.py` → `{db.DB_PATH}` | "
            f"`app.py` → `{DATABASE_FILE}`. Ambos devem apontar para o mesmo "
            "`database.db` gerado pelo `seed_db.py`."
        )
    prev = st.session_state.pop("_sqlite_conn_active", None)
    if prev is not None:
        try:
            prev.close()
        except Exception:
            pass
    conn = db.init_database()
    st.session_state["_sqlite_conn_active"] = conn
    return conn


def main() -> None:
    st.set_page_config(
        page_title="CFO Pessoal — Auditax",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown("""
<style>
    /* 1. Reset e Estilo Global (Modo Dark Auditax) */
    html, body, [data-testid="stAppViewContainer"] {
        background-color: #121212 !important;
        color: #E0E0E0 !important;
        font-family: 'Inter', sans-serif !important;
    }

    /* 2. Sidebar Customizada */
    [data-testid="stSidebar"] {
        background-color: #1E1E1E !important;
        border-right: 1px solid #333333;
    }
    [data-testid="stSidebarNav"] span {
        color: #B0B0B0 !important;
        font-weight: 500;
    }

    /* 3. Cards Premium (Padrão da Imagem de Referência) */
    div[data-testid="stColumn"] > div {
        background-color: #1E1E1E;
        padding: 24px;
        border-radius: 16px;
        border: 1px solid #333333;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
        margin-bottom: 20px;
    }

    /* 4. Métricas Estilizadas */
    [data-testid="stMetricValue"] {
        font-size: 32px !important;
        font-weight: 700 !important;
        color: #FFFFFF !important;
    }
    [data-testid="stMetricLabel"] {
        font-size: 14px !important;
        color: #B0B0B0 !important;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    [data-testid="stMetricDelta"] {
        color: #4CAF50 !important;
    }

    /* 5. Títulos de Cards e Gráficos */
    h3 {
        font-size: 18px !important;
        color: #FFFFFF !important;
        font-weight: 600 !important;
        margin-bottom: 20px !important;
    }

    /* 6. Barras de Progresso Customizadas */
    .stProgress > div > div > div > div {
        background-color: #0D47A1 !important;
        border-radius: 10px;
    }
    .stProgress {
        height: 10px !important;
    }

    /* 7. Botões e Inputs */
    .stButton > button {
        background-color: #0D47A1 !important;
        color: white !important;
        border-radius: 8px !important;
        border: none !important;
        padding: 10px 24px !important;
    }
</style>
""", unsafe_allow_html=True)
    st.title("CFO Pessoal — Iago (Auditax)")
    conn = get_conn()

    try:
        n_clientes_db = conn.execute(
            "SELECT COUNT(*) FROM clientes"
        ).fetchone()[0]
    except sqlite3.OperationalError as exc:
        st.error(
            "**Table not found** (ou banco corrompido/incompatível). "
            f"{_hint_sql_erro(exc)}"
        )
        st.stop()

    _using_pg = db._read_pg_url() is not None
    with st.sidebar:
        st.header("Base de dados")
        if _using_pg:
            st.success("Conectado ao **Supabase** (PostgreSQL)")
        else:
            st.success(
                f"Conectado ao **`database.db`** (SQLite local):\n\n"
                f"`{DATABASE_FILE}`"
            )
            if not DATABASE_FILE.exists():
                st.warning("`database.db` ainda não existe. Rode `python3 seed_db.py`.")
        st.caption(f"Registros em `clientes`: **{int(n_clientes_db)}**")
        if not _using_pg and st.button("Recarregar a partir dos CSV", type="secondary"):
            st.session_state.pop("_sqlite_conn_active", None)
            try:
                conn.close()
            except Exception:
                pass
            if DATABASE_FILE.exists():
                os.remove(DATABASE_FILE)
            db.init_database(force_reload=True)
            st.rerun()

    tab_dash, tab_desp, tab_cli, tab_div, tab_car, tab_ee, tab_flux, tab_rp = st.tabs(
        [
            "Dashboard",
            "Despesas",
            "Clientes",
            "Dívidas",
            "Cartões",
            "Entradas Extras",
            "Fluxo de Caixa Projetado",
            "Realizado vs. Projetado",
        ]
    )

    with tab_dash:
        mes_ref = st.date_input(
            "Mês de referência (performance, saídas e recortes)",
            value=date.today().replace(day=1),
            key="dash_mes_ref",
        )
        ym_dash = _ym_ref(mes_ref)
        fat = db.faturamento_mensal_total(conn)
        tot_ee = db.total_entradas_extras(conn)
        saldo_caixa = db.saldo_caixa_total(conn)
        saldo_previsto = db.saldo_caixa_previsto(conn)
        n_ee_prov = db.count_entradas_extras_provisionadas(conn)
        val_ee_prov = db.total_entradas_extras_so_provisionadas(conn)
        val_desp_prov = db.total_despesas_provisionadas_nao_realizadas(conn)
        try:
            ee_fat_mes = db.total_entradas_extras_mes_categorias(
                conn, ym_dash, EE_CATEGORIAS_FATURAMENTO_MES
            )
            ee_cap_mes = db.total_entradas_extras_mes_categorias(
                conn, ym_dash, EE_CATEGORIAS_MOV_CAPITAL
            )
        except sqlite3.OperationalError:
            ee_fat_mes = 0.0
            ee_cap_mes = 0.0
        perf_faturamento_mes = float(fat) + float(ee_fat_mes)
        saidas_deb = db.total_saidas_debito_mes(conn, ym_dash)
        saidas_car = db.total_saidas_cartao_mes(conn, ym_dash)
        try:
            prov_mes = db.total_provisoes_mes(conn, ym_dash)
        except sqlite3.OperationalError:
            prov_mes = 0.0
        total_despesas_mes = saidas_deb + saidas_car + prov_mes

        # ── Hero: 4 cards principais ─────────────────────────────────────────
        h1, h2, h3, h4 = st.columns(4)
        with h1:
            st.metric(
                "Em Caixa",
                brl(saldo_caixa),
                help="Transações realizadas (`realizado = 1`) + entradas extras **Realizadas**.",
            )
        with h2:
            st.metric(
                "A Receber (mês)",
                brl(perf_faturamento_mes),
                help=(
                    f"Honorários da carteira ativa + Receitas Eventual / IR no mês **{ym_dash}**. "
                    "Representa a expectativa de entrada para o mês de referência."
                ),
            )
        with h3:
            st.metric(
                "Despesas (mês)",
                brl(total_despesas_mes),
                help=(
                    f"Saídas débito + faturas de cartão + provisões provisionadas no mês **{ym_dash}**."
                ),
            )
        with h4:
            st.metric(
                "Projeção de Saldo ⚡",
                brl(saldo_previsto),
                delta=brl(saldo_previsto - saldo_caixa),
                delta_color="normal",
                help=(
                    "**Fórmula:** Saldo Real + Entradas Extras Provisionadas − Despesas Provisionadas (não realizadas).\n\n"
                    f"• Saldo Real: {brl(saldo_caixa)}\n"
                    f"• + EE Provisionadas: {brl(val_ee_prov)}\n"
                    f"• − Desp. Provisionadas: {brl(val_desp_prov)}\n\n"
                    "Para projeção dia a dia, consulte a aba **Fluxo de Caixa**."
                ),
            )

        st.divider()

        # ── Detalhamento de performance e capital ────────────────────────────
        st.subheader("Performance e faturamento (mês selecionado)")
        st.caption(
            "**Receita Eventual** e **Receita Imposto de Renda** entram no **total de performance** "
            "com os honorários mensais da carteira. **Aportes / resgates / empréstimo / outros** só movem caixa."
        )
        d1, d2, d3 = st.columns(3)
        with d1:
            st.metric(
                "Honorários (carteira ativa, mensal)",
                brl(fat),
                help="Soma de `valor_honorario` em clientes Ativos — base recorrente Auditax.",
            )
        with d2:
            st.metric(
                "Receitas Eventual + IR (no mês)",
                brl(ee_fat_mes),
                help=f"Lançamentos em `entradas_extras` no mês **{ym_dash}** nas categorias "
                f"{', '.join(EE_CATEGORIAS_FATURAMENTO_MES)}.",
            )
        with d3:
            st.metric(
                "Total performance (mês)",
                brl(perf_faturamento_mes),
                help="Honorários mensais + Receita Eventual + Imposto de Renda (competência pela **data** do lançamento).",
            )

        st.subheader("Movimentação de capital e caixa")
        d4, d5 = st.columns(2)
        with d4:
            st.metric(
                "Capital no mês (Aporte, Resgate, Empréstimo, Outros)",
                brl(ee_cap_mes),
                help="Entradas extras no mês que **não** entram no indicador de faturamento acima; afetam o saldo.",
            )
        with d5:
            st.metric(
                "Entradas Extras (histórico total)",
                brl(tot_ee),
                help="Soma acumulada de todas as categorias em `entradas_extras` com status **Realizado**.",
            )
            if n_ee_prov > 0:
                st.markdown(
                    f'<span style="'
                    f'background:#1d4ed8;color:#fff;'
                    f'padding:3px 10px;border-radius:999px;'
                    f'font-size:12px;font-weight:600;letter-spacing:.4px;">'
                    f'⏳ {n_ee_prov} Provisionada{"s" if n_ee_prov > 1 else ""}'
                    f' · {brl(val_ee_prov)}'
                    f'</span>',
                    unsafe_allow_html=True,
                )

        st.info(
            "**Honorários** refletem a carteira ativa. "
            "**Receita Eventual** e **Imposto de Renda** são receitas extraordinárias no **Total performance** do mês. "
            "**Aportes e resgates** recompõem saldo, sem compor faturamento. "
            "Lançamentos operacionais: **Despesas** e **Cartões**."
        )

        st.subheader("Visão de saídas do mês")
        g1, g2 = st.columns(2)
        with g1:
            st.plotly_chart(
                fig_pie_saidas_tres_grupos(saidas_deb, prov_mes, saidas_car),
                use_container_width=True,
            )
        with g2:
            cats = db.categorias_saidas_mes(conn, ym_dash)
            st.plotly_chart(
                fig_pie_categorias_saidas(cats),
                use_container_width=True,
            )
        st.caption(
            "**Provisões:** na aba Despesas (únicas ou série **[REC]** mensal), **não realizadas**, "
            "com **data prevista** caindo no mês escolhido — aparecem neste gráfico e no fluxo até **Realizar**. "
            "**Despesas (débito):** saídas pela conta no mês (data do lançamento). "
            "**Cartões:** compras cuja fatura é do mês (`mes_fatura` ou, se vazio, data no mês)."
        )

    with tab_desp:
        st.subheader("Despesas — caixa, provisões e realização")
        if st.session_state.pop("_desp_ok", None):
            st.success("Despesa salva em `database.db` (`transacoes`).")
        if st.session_state.pop("_prov_ok", None):
            n_prev = st.session_state.pop("_prov_ok_n", 1)
            st.success(
                f"**{int(n_prev)}** provisões registradas."
                if int(n_prev) > 1
                else "Provisão registrada."
            )
        if st.session_state.pop("_real_ok", None):
            st.success("Provisão realizada; lançamento confirmado em `transacoes`.")
        st.caption(
            "**Provisão:** agenda uma saída futura (aparece no fluxo como **prevista** até você realizar). "
            "**Realizar:** gera a saída **real** na conta. **Parcela de dívida:** provisione ou lance direto "
            "aqui para o caixa; depois registre o abatimento na aba **Dívidas**. Compras no crédito: aba **Cartões**."
        )

        contas_df = db.read_sql(
            "SELECT id, nome FROM contas_bancarias ORDER BY nome COLLATE NOCASE",
            conn,
        )
        id_list = [int(x) for x in contas_df["id"].tolist()] if not contas_df.empty else []
        nome_por_id = (
            {int(r["id"]): str(r["nome"]) for _, r in contas_df.iterrows()}
            if not contas_df.empty
            else {}
        )

        st.markdown("##### Provisionar despesa futura")
        with st.form("form_provisao_despesa"):
            p1, p2 = st.columns(2)
            with p1:
                p_data = st.date_input(
                    "Data prevista (1ª parcela)",
                    value=date(2026, 4, 5),
                    key="prov_data",
                    help="Para recorrência mensal, é a data do 1º vencimento; as demais avançam 1 mês cada.",
                )
            with p2:
                p_val = st.number_input(
                    "Valor previsto (R$)",
                    min_value=0.0,
                    value=0.0,
                    format="%.2f",
                    key="prov_val",
                )
            p3, p4 = st.columns(2)
            with p3:
                p_rec = st.selectbox(
                    "Recorrência",
                    options=["Única", "Mensal"],
                    key="prov_recorrencia",
                )
            with p4:
                n_meses_rec = st.number_input(
                    "Quantidade de meses (só se Mensal)",
                    min_value=1,
                    max_value=120,
                    value=12,
                    step=1,
                    key="prov_n_meses",
                    disabled=(p_rec == "Única"),
                    help="Ex.: 12 gera 12 linhas: data inicial + 11 meses seguintes. Descrição com prefixo **[REC]**.",
                )
            p_desc = st.text_input("Descrição", key="prov_desc")
            p_cat = st.selectbox(
                "Categoria",
                options=list(CATEGORIAS_DESPESA_DEBITO),
                key="prov_cat",
            )
            st.caption(
                "_Recorrência **Mensal** grava várias provisões; cada linha fica com descrição **`[REC] …`** "
                "e aparece no **Dashboard** e no **fluxo** no mês da respectiva **data prevista** até você **realizar**._"
            )
            if st.form_submit_button("Salvar provisão", type="primary"):
                if not (p_desc or "").strip():
                    st.error("Informe a descrição.")
                elif p_val <= 0:
                    st.error("Informe o valor previsto.")
                elif p_rec == "Mensal" and n_meses_rec < 1:
                    st.error("Informe a quantidade de meses.")
                else:
                    try:
                        n_ins = db.insert_provisoes_recorrentes(
                            conn,
                            data_prevista_inicial_iso=p_data.isoformat(),
                            descricao=p_desc.strip(),
                            valor_previsto=float(p_val),
                            categoria=p_cat,
                            recorrencia_mensal=(p_rec == "Mensal"),
                            quantidade_meses=int(n_meses_rec),
                        )
                        st.session_state["_prov_ok"] = True
                        st.session_state["_prov_ok_n"] = int(n_ins)
                        st.rerun()
                    except (ValueError, sqlite3.OperationalError) as e:
                        st.error(
                            str(e) if isinstance(e, ValueError) else _hint_sql_erro(e)
                        )

        st.divider()
        st.markdown("##### Realizar provisão (gera lançamento em conta)")
        try:
            pend = db.read_sql(
                """
                SELECT id, data_prevista, descricao, valor_previsto, categoria
                FROM despesas_provisionadas
                WHERE realizado = 0
                ORDER BY date(data_prevista), id
                """,
                conn,
            )
        except sqlite3.OperationalError as exc:
            st.error(_hint_sql_erro(exc))
            pend = pd.DataFrame()

        if contas_df.empty:
            st.warning(
                "Cadastre uma conta em `contas_bancarias` para realizar provisões e lançar débito direto."
            )
        elif pend.empty:
            st.caption("_Nenhuma provisão pendente._")
        else:
            opts = {
                int(r["id"]): f"{str(r['data_prevista'])[:10]} | {str(r['descricao'])[:55]} | {brl(float(r['valor_previsto']))}"
                for _, r in pend.iterrows()
            }
            with st.form("form_realizar_provisao"):
                pid = st.selectbox(
                    "Provisão",
                    options=list(opts.keys()),
                    format_func=lambda k: opts[int(k)],
                    key="real_sel_prov",
                )
                pref = pend[pend["id"] == pid].iloc[0]
                r1, r2 = st.columns(2)
                with r1:
                    r_data = st.date_input(
                        "Data real do pagamento",
                        value=date.fromisoformat(str(pref["data_prevista"])[:10]),
                        key="real_data",
                    )
                with r2:
                    r_val = st.number_input(
                        "Valor real (R$)",
                        min_value=0.01,
                        value=float(pref["valor_previsto"]),
                        format="%.2f",
                        key="real_val",
                    )
                r_conta = st.selectbox(
                    "Conta (débito)",
                    options=id_list,
                    format_func=lambda i: nome_por_id.get(int(i), str(i)),
                    key="real_conta",
                )
                if st.form_submit_button("Realizar e confirmar em transações", type="primary"):
                    try:
                        db.realizar_despesa_provisionada(
                            conn,
                            int(pid),
                            data_real_iso=r_data.isoformat(),
                            valor_real=float(r_val),
                            conta_bancaria_id=int(r_conta),
                        )
                        st.session_state["_real_ok"] = True
                        st.rerun()
                    except (ValueError, sqlite3.OperationalError) as e:
                        st.error(str(e))

        st.divider()
        st.markdown("##### Lançamento direto (despesa já paga)")
        if contas_df.empty:
            pass
        else:
            with st.form("form_despesa_debito"):
                r1, r2 = st.columns(2)
                with r1:
                    d_data = st.date_input("Data", value=date.today(), key="desp_data")
                with r2:
                    d_val = st.number_input(
                        "Valor (R$)",
                        min_value=0.01,
                        value=50.0,
                        format="%.2f",
                        key="desp_val",
                    )
                d_desc = st.text_input("Descrição", key="desp_desc")
                d_cat = st.selectbox(
                    "Categoria",
                    options=list(CATEGORIAS_DESPESA_DEBITO),
                    key="desp_cat",
                )
                d_conta = st.selectbox(
                    "Conta",
                    options=id_list,
                    format_func=lambda i: nome_por_id.get(int(i), str(i)),
                    key="desp_conta",
                )
                if st.form_submit_button("Lançar despesa", type="primary"):
                    if not (d_desc or "").strip():
                        st.error("Informe a descrição.")
                    else:
                        db.insert_despesa_debito(
                            conn,
                            data=d_data.isoformat(),
                            descricao=d_desc.strip(),
                            valor_abs=float(d_val),
                            categoria=d_cat,
                            conta_bancaria_id=int(d_conta),
                        )
                        st.session_state["_desp_ok"] = True
                        st.rerun()

        try:
            hist_prov = db.read_sql(
                """
                SELECT id, data_prevista, descricao, valor_previsto, realizado,
                       data_realizada, valor_real
                FROM despesas_provisionadas
                ORDER BY id DESC
                LIMIT 80
                """,
                conn,
            )
        except sqlite3.OperationalError:
            hist_prov = pd.DataFrame()
        if not hist_prov.empty:
            with st.expander("Histórico de provisões (últimas 80)", expanded=False):
                st.dataframe(hist_prov, hide_index=True, use_container_width=True)

    with tab_cli:
        st.subheader("Honorários do mês")
        ref = st.date_input(
            "Mês de competência",
            value=date(2026, 3, 1),
            help="Usa o primeiro dia do mês selecionado como data_competência.",
        )
        competencia = date(ref.year, ref.month, 1).isoformat()

        df = db.read_sql(
            """
            SELECT
              c.id AS cliente_id,
              c.nome,
              c.valor_honorario,
              COALESCE(r.status, 'Pendente') AS status_pagamento
            FROM clientes c
            LEFT JOIN receitas r
              ON r.cliente_id = c.id AND r.data_competencia = ?
            WHERE c.status = 'Ativo'
            ORDER BY c.nome
            """,
            conn,
            params=(competencia,),
        )
        edited = st.data_editor(
            df,
            column_config={
                "cliente_id": st.column_config.NumberColumn("ID", disabled=True, format="%d"),
                "nome": st.column_config.TextColumn("Cliente", disabled=True, width="large"),
                "valor_honorario": st.column_config.NumberColumn(
                    "Honorário",
                    disabled=True,
                    format="R$ %.2f",
                ),
                "status_pagamento": st.column_config.SelectboxColumn(
                    "Pago no mês?",
                    options=["Pendente", "Pago"],
                    required=True,
                ),
            },
            hide_index=True,
            num_rows="fixed",
            key="grid_clientes",
        )
        if st.button("Salvar pagamentos do mês", type="primary"):
            for _, row in edited.iterrows():
                db.upsert_receita_mes(
                    conn,
                    cliente_id=int(row["cliente_id"]),
                    data_competencia=competencia,
                    status=str(row["status_pagamento"]),
                )
            st.success("Status de receitas atualizado.")
            st.rerun()

    with tab_div:
        st.subheader("Dívidas — cadastro e amortização patrimonial")
        st.info(
            "**Lembrete:** O pagamento registrado **aqui** abate apenas o **saldo devedor** "
            "(controle patrimonial). Para o valor **sair do caixa** no mês, lance a parcela na aba "
            "**DESPESAS** (provisão ou débito direto)."
        )
        if st.session_state.pop("_div_pag_ok", None):
            st.success("Pagamento registrado — saldo devedor atualizado.")
        if st.session_state.pop("_div_new_ok", None):
            st.success("Nova dívida salva em `dividas_emprestimos`.")

        try:
            ddf = db.read_sql(
                """
                SELECT
                  id,
                  tipo,
                  instituicao,
                  descricao,
                  valor_total,
                  taxa_juros_mensal_pct,
                  taxa_implicita,
                  valor_parcela,
                  parcelas_restantes,
                  saldo_quitacao,
                  prioridade,
                  ativo,
                  termino_previsto,
                  observacoes
                FROM dividas_emprestimos
                ORDER BY prioridade IS NULL, prioridade ASC,
                         valor_total DESC
                """,
                conn,
            )
        except sqlite3.OperationalError as exc:
            st.error(_hint_sql_erro(exc))
            ddf = pd.DataFrame()

        with st.expander("Cadastrar Nova Dívida", expanded=False):
            st.caption(
                "O **id** é gerado automaticamente. `parcelas_restantes` inicia igual ao total de parcelas."
            )
            with st.form("form_nova_divida"):
                c1, c2 = st.columns(2)
                with c1:
                    nd_inst = st.text_input("Instituição", placeholder="Ex.: Mercado Pago, Itaú")
                    nd_desc = st.text_input("Descrição", placeholder="Ex.: Crédito pessoal")
                with c2:
                    nd_vtotal = st.number_input(
                        "Valor total (principal)",
                        min_value=0.01,
                        value=1000.0,
                        format="%.2f",
                    )
                    nd_tx = st.number_input(
                        "Taxa de juros mensal (%)",
                        min_value=0.0,
                        value=0.0,
                        format="%.4f",
                        help="Informe como percentual (ex.: 2,5 para 2,5% a.m.). Use 0 se não souber.",
                    )
                c3, c4 = st.columns(2)
                with c3:
                    nd_vp = st.number_input(
                        "Valor da parcela",
                        min_value=0.01,
                        value=100.0,
                        format="%.2f",
                    )
                with c4:
                    nd_nparc = st.number_input(
                        "Total de parcelas",
                        min_value=1,
                        value=12,
                        step=1,
                    )
                if st.form_submit_button("Salvar nova dívida", type="primary"):
                    try:
                        taxa_sql = float(nd_tx) if nd_tx and nd_tx > 0 else None
                        db.insert_divida_emprestimo(
                            conn,
                            instituicao=nd_inst,
                            descricao=nd_desc,
                            valor_total=float(nd_vtotal),
                            taxa_juros_mensal_pct=taxa_sql,
                            valor_parcela=float(nd_vp),
                            parcelas_total=int(nd_nparc),
                        )
                        st.session_state["_div_new_ok"] = True
                        st.rerun()
                    except (ValueError, sqlite3.OperationalError) as e:
                        st.error(
                            str(e) if isinstance(e, ValueError) else _hint_sql_erro(e)
                        )

        if ddf.empty:
            st.info(
                "Nenhuma dívida cadastrada. Use **Cadastrar Nova Dívida** acima ou rode `python3 seed_db.py` "
                "para importar o CSV."
            )
        else:
            st.caption(
                "**Atenção:** amortização aqui não movimenta o fluxo de caixa — apenas `pagamentos_dividas`."
            )
            st.markdown("##### Dívidas ativas — progresso e parcelas")
            data_parc_batch = st.date_input(
                "Data do pagamento (para **Registrar Pagamento de Parcela**)",
                value=date.today(),
                key="div_data_parc_lote",
            )

            ddf_at = ddf[ddf["ativo"].astype(int) == 1].copy()
            if ddf_at.empty:
                st.warning("Nenhuma dívida com `ativo = 1`. Ajuste no detalhamento abaixo ou reative um contrato.")
            else:
                for _, row in ddf_at.iterrows():
                    did = int(row["id"])
                    try:
                        tp = db.total_pagamentos_divida(conn, did)
                    except sqlite3.OperationalError:
                        tp = 0.0
                    try:
                        n_lan = int(
                            conn.execute(
                                "SELECT COUNT(*) AS c FROM pagamentos_dividas WHERE divida_id = ?",
                                (did,),
                            ).fetchone()[0]
                        )
                    except sqlite3.OperationalError:
                        n_lan = 0
                    ini = float(row["valor_total"])
                    rem = max(0.0, ini - tp)
                    prog, cet_txt = _divida_progresso_e_cet_md(row, tp, n_lan)
                    tit = f"**{row['instituicao']}** — {str(row['descricao'])[:50]}"
                    st.markdown(tit)
                    st.progress(float(prog))
                    st.caption(
                        f"Progresso: **{brl(tp)}** amortizado de **{brl(ini)}** "
                        f"({100.0 * prog:.0f}% do principal)."
                    )
                    k0, k1, k2 = st.columns(3)
                    with k0:
                        st.metric("Principal (contrato)", brl(ini))
                    with k1:
                        st.metric("Total amortizado", brl(tp))
                    with k2:
                        st.metric("Saldo devedor", brl(rem))
                    if cet_txt:
                        st.markdown(cet_txt)
                    bcol1, bcol2 = st.columns([1, 2])
                    with bcol1:
                        parc_disabled = rem <= 0
                        if st.button(
                            "Registrar Pagamento de Parcela",
                            key=f"btn_parcela_{did}",
                            disabled=parc_disabled,
                            help="Abate o valor da parcela (ou o saldo remanescente) neste contrato, sem lançar em Despesas.",
                        ):
                            try:
                                db.registrar_parcela_divida(
                                    conn,
                                    did,
                                    data_pagamento_iso=data_parc_batch.isoformat(),
                                )
                                st.session_state["_div_pag_ok"] = True
                                st.rerun()
                            except ValueError as e:
                                st.error(str(e))
                            except sqlite3.OperationalError as exc:
                                st.error(_hint_sql_erro(exc))
                    with bcol2:
                        st.caption(
                            f"Próximo lançamento sugerido: **{brl(min(float(row['valor_parcela']), rem))}** "
                            "(valor da parcela ou saldo, o que for menor)."
                        )

                    with st.expander(f"Histórico e abatimento manual — id {did}", expanded=False):
                        try:
                            pags = db.read_sql(
                                """
                                SELECT data_pagamento, valor, observacao, created_at
                                FROM pagamentos_dividas
                                WHERE divida_id = ?
                                ORDER BY date(data_pagamento) DESC, id DESC
                                """,
                                conn,
                                params=(did,),
                            )
                        except sqlite3.OperationalError:
                            pags = pd.DataFrame()
                        if not pags.empty:
                            st.dataframe(
                                pags,
                                column_config={
                                    "valor": st.column_config.NumberColumn(format="R$ %.2f"),
                                },
                                hide_index=True,
                                use_container_width=True,
                            )
                        with st.form(f"form_pag_div_manual_{did}"):
                            pg1, pg2 = st.columns(2)
                            with pg1:
                                pg_data = st.date_input(
                                    "Data",
                                    value=date.today(),
                                    key=f"pgd_m_{did}",
                                )
                            with pg2:
                                pg_val = st.number_input(
                                    "Outro valor (abatimento)",
                                    min_value=0.01,
                                    value=float(row["valor_parcela"] or 0.01),
                                    format="%.2f",
                                    key=f"pgv_m_{did}",
                                )
                            pg_obs = st.text_input("Observação", key=f"pgo_m_{did}")
                            if st.form_submit_button("Registrar este valor"):
                                try:
                                    db.insert_pagamento_divida(
                                        conn,
                                        divida_id=did,
                                        data_pagamento_iso=pg_data.isoformat(),
                                        valor=float(pg_val),
                                        observacao=(pg_obs or "").strip() or None,
                                    )
                                    st.session_state["_div_pag_ok"] = True
                                    st.rerun()
                                except sqlite3.OperationalError as exc:
                                    st.error(_hint_sql_erro(exc))

                    st.divider()

            st.subheader("Detalhamento (planilha)")
            st.caption(
                "**Economia (R$)** = `valor_total - saldo_quitacao` (quando há valor de quitação no CSV). "
                "**% a.a. nominal** derivada da **taxa real % a.m.** do CSV "
                "(ignorada se `taxa_implicita = 1`)."
            )
            ddf_det = ddf.copy()

            def _econ_row(r: pd.Series) -> float | None:
                sq = r.get("saldo_quitacao")
                if sq is None or (isinstance(sq, float) and math.isnan(sq)) or pd.isna(sq):
                    return None
                return float(r["valor_total"]) - float(sq)

            ddf_det["economia_antecipacao_rs"] = ddf_det.apply(_econ_row, axis=1)
            ddf_det["taxa_aa_nominal_pct"] = ddf_det.apply(
                lambda r: _taxa_aa_nominal_pct(
                    r.get("taxa_juros_mensal_pct"),
                    int(float(r.get("taxa_implicita") or 0)),
                ),
                axis=1,
            )
            st.dataframe(
                ddf_det,
                column_config={
                    "valor_total": st.column_config.NumberColumn(format="R$ %.2f"),
                    "valor_parcela": st.column_config.NumberColumn(format="R$ %.2f"),
                    "saldo_quitacao": st.column_config.NumberColumn(format="R$ %.2f"),
                    "economia_antecipacao_rs": st.column_config.NumberColumn(
                        "Economia (antecip.) R$",
                        format="R$ %.2f",
                    ),
                    "taxa_juros_mensal_pct": st.column_config.NumberColumn(
                        "% a.m. (CSV)",
                        format="%.4f",
                    ),
                    "taxa_aa_nominal_pct": st.column_config.NumberColumn(
                        "% a.a. (derivado)",
                        format="%.2f",
                    ),
                    "ativo": st.column_config.NumberColumn("ativo (0/1)", format="%d"),
                },
                hide_index=True,
                use_container_width=True,
            )

    with tab_car:
        st.subheader("Gestão de liquidação — faturas de cartão")
        if st.session_state.pop("_fat_ok", None):
            st.success("Fatura registrada em `database.db`.")
        if st.session_state.pop("_fat_pay_ok", None):
            st.success("Fatura paga; saída lançada em `transacoes`.")
        st.caption(
            "Fechamento mensal (valor total da fatura), matriz cartão × mês, e baixa com geração "
            "automática do pagamento. **Fluxo projetado:** faturas **não pagas** entram como saída no "
            "**dia do vencimento**; ao pagar, a projeção some e a saída real vai para a conta escolhida."
        )

        if st.session_state.pop("_cart_novo_ok", None):
            st.success("Cartão salvo em `cartoes_credito`.")

        with st.expander("Cadastrar Novo Cartão de Crédito", expanded=False):
            with st.form("form_novo_cartao"):
                cn = st.text_input(
                    "Nome do cartão",
                    placeholder="Ex.: Nubank, XP",
                    key="cart_nome",
                )
                cl = st.number_input(
                    "Limite (R$) — opcional",
                    min_value=0.0,
                    value=0.0,
                    format="%.2f",
                    key="cart_lim",
                )
                dv = st.number_input(
                    "Dia do vencimento no mês",
                    min_value=1,
                    max_value=31,
                    value=10,
                    key="cart_dv",
                )
                if st.form_submit_button("Salvar cartão"):
                    if not (cn or "").strip():
                        st.error("Informe o nome do cartão.")
                    else:
                        try:
                            lim_v = float(cl) if cl and cl > 0 else None
                            db.insert_cartao_credito(
                                conn,
                                nome=cn.strip(),
                                limite=lim_v,
                                dia_vencimento=int(dv),
                                melhor_dia_compra=None,
                            )
                            st.session_state["_cart_novo_ok"] = True
                            st.rerun()
                        except (ValueError, sqlite3.OperationalError) as e:
                            st.error(
                                str(e)
                                if isinstance(e, ValueError)
                                else _hint_sql_erro(e)
                            )

        cart_df = db.read_sql(
            """
            SELECT id, nome FROM cartoes_credito
            ORDER BY nome COLLATE NOCASE
            """,
            conn,
        )
        cart_ids = (
            [int(x) for x in cart_df["id"].tolist()] if not cart_df.empty else []
        )
        nomes_por_id = (
            {int(r["id"]): str(r["nome"]) for _, r in cart_df.iterrows()}
            if not cart_df.empty
            else {}
        )

        with st.form("form_fechamento_fatura"):
            st.markdown("##### Lançar fechamento do mês")
            fc0, fc1 = st.columns(2)
            with fc0:
                if cart_ids:
                    sel_cid = st.selectbox(
                        "Cartão",
                        options=cart_ids,
                        format_func=lambda i: nomes_por_id.get(int(i), str(i)),
                        key="fat_sel_cid",
                    )
                else:
                    sel_cid = None
                    st.warning("Cadastre um cartão acima ou via seed.")
            with fc1:
                mes_ref_in = st.date_input(
                    "Mês/ano de referência (competência da fatura)",
                    value=date.today().replace(day=1),
                    key="fat_mes_ref",
                )
            fc2, fc3 = st.columns(2)
            with fc2:
                venc = st.date_input(
                    "Data exata do vencimento",
                    value=date.today(),
                    key="fat_venc",
                )
            with fc3:
                vtot = st.number_input(
                    "Valor total da fatura (R$)",
                    min_value=0.0,
                    value=0.0,
                    format="%.2f",
                    key="fat_vtot",
                )
            if st.form_submit_button("Salvar fechamento", type="primary"):
                if not cart_ids or sel_cid is None:
                    st.error("Cadastre um cartão no banco.")
                elif vtot <= 0:
                    st.error("Informe o valor total da fatura.")
                else:
                    try:
                        mes_iso = date(mes_ref_in.year, mes_ref_in.month, 1).isoformat()
                        db.insert_or_update_fatura_fechamento(
                            conn,
                            cartao_id=int(sel_cid),
                            mes_referencia_iso=mes_iso,
                            data_vencimento_iso=venc.isoformat(),
                            valor_total=float(vtot),
                        )
                        st.session_state["_fat_ok"] = True
                        st.rerun()
                    except ValueError as e:
                        st.error(str(e))

        st.divider()
        st.markdown("##### Matriz: cartões × meses")
        mx1, mx2 = st.columns(2)
        with mx1:
            mat_ini = st.date_input(
                "Primeiro mês (coluna esquerda)",
                value=date.today().replace(day=1),
                key="mat_ini",
            )
        with mx2:
            n_meses_mat = st.number_input(
                "Quantidade de meses (colunas)",
                min_value=3,
                max_value=24,
                value=12,
                key="n_meses_mat",
            )
        col_keys = _meses_colunas(mat_ini, int(n_meses_mat))
        col_labels = [k[:7] for k in col_keys]
        try:
            fat_all = db.read_sql(
                """
                SELECT cartao_id, mes_referencia, valor_total, status_pago
                FROM faturas_pagas
                """,
                conn,
            )
        except sqlite3.OperationalError:
            fat_all = pd.DataFrame(
                columns=["cartao_id", "mes_referencia", "valor_total", "status_pago"]
            )

        mat_rows: list[dict[str, str]] = []
        if cart_df.empty:
            mat_rows.append({"Cartão": "—", **{cl: "—" for cl in col_labels}})
        else:
            for _, crow in cart_df.iterrows():
                cid = int(crow["id"])
                label = str(crow["nome"])
                rowd: dict[str, str] = {"Cartão": label}
                for cl, mk in zip(col_labels, col_keys):
                    sub = fat_all[
                        (fat_all["cartao_id"] == cid)
                        & (fat_all["mes_referencia"].astype(str).str[:10] == mk[:10])
                    ]
                    if sub.empty:
                        rowd[cl] = "—"
                    else:
                        v = float(sub.iloc[0]["valor_total"])
                        pago = int(sub.iloc[0]["status_pago"])
                        rowd[cl] = f"{brl(v)} ✅" if pago else brl(v)
                mat_rows.append(rowd)
        st.dataframe(pd.DataFrame(mat_rows), hide_index=True, use_container_width=True)

        st.divider()
        st.markdown("##### Faturas pendentes — pagar")
        contas_pg = db.read_sql(
            "SELECT id, nome FROM contas_bancarias ORDER BY nome COLLATE NOCASE",
            conn,
        )
        if contas_pg.empty:
            st.warning("Cadastre uma conta em `contas_bancarias` para lançar o pagamento.")
            conta_pg_id = None
        else:
            idl = [int(x) for x in contas_pg["id"].tolist()]
            nm = {int(r["id"]): str(r["nome"]) for _, r in contas_pg.iterrows()}
            conta_pg_id = st.selectbox(
                "Conta para débito do pagamento",
                options=idl,
                format_func=lambda i: nm.get(int(i), str(i)),
                key="fat_conta_pagar",
            )

        pendentes = []
        try:
            pendentes = db.faturas_pendentes(conn)
        except sqlite3.OperationalError:
            pass
        if not pendentes:
            st.info("Nenhuma fatura pendente.")
        else:
            for fp in pendentes:
                fid = int(fp["id"])
                st.write(
                    f"**{fp['cartao_nome']}** — {brl(float(fp['valor_total']))} — "
                    f"venc. **{fp['data_vencimento'][:10]}** — ref. **{str(fp['mes_referencia'])[:7]}**"
                )
                if conta_pg_id is not None and st.button(
                    "Pagar fatura",
                    key=f"pagar_fat_{fid}",
                    type="primary",
                ):
                    try:
                        db.pagar_fatura(conn, fid, int(conta_pg_id))
                        st.session_state["_fat_pay_ok"] = True
                        st.rerun()
                    except ValueError as e:
                        st.error(str(e))

        with st.expander("Cadastro completo de cartões (`cartoes_credito`)", expanded=False):
            try:
                cartoes_db = db.read_sql(
                    """
                    SELECT id, nome, limite, melhor_dia_compra, dia_vencimento, created_at
                    FROM cartoes_credito
                    ORDER BY nome COLLATE NOCASE
                    """,
                    conn,
                )
            except sqlite3.OperationalError as exc:
                st.error(_hint_sql_erro(exc).replace("dividas_emprestimos", "cartoes_credito"))
                cartoes_db = pd.DataFrame()
            if cartoes_db.empty:
                st.caption("_Tabela vazia._")
            else:
                st.dataframe(cartoes_db, hide_index=True, use_container_width=True)

        st.subheader("Resumo histórico — lançamentos em `transacoes` (cartão)")
        st.caption(
            "Soma de lançamentos com `cartao_id` por cartão cadastrado (compras/itens)."
        )
        resumo = []
        if not cart_df.empty:
            for _, crow in cart_df.iterrows():
                cid = int(crow["id"])
                label = str(crow["nome"])
                total = db.soma_fatura_cartao(conn, cid)
                resumo.append(
                    {
                        "Cartão": label,
                        "cartao_id": cid,
                        "Total lançamentos": total,
                    }
                )
        cdf = pd.DataFrame(resumo)
        if cdf["Total lançamentos"].sum() == 0:
            st.caption("_Nenhum lançamento com cartão em `transacoes`._")
        st.dataframe(
            cdf,
            column_config={
                "Total lançamentos": st.column_config.NumberColumn(format="R$ %.2f"),
            },
            hide_index=True,
            use_container_width=True,
        )

    with tab_ee:
        st.subheader("Entradas Extras")
        if st.session_state.pop("_ee_cadastro_ok", None):
            n_e = int(st.session_state.pop("_ee_cadastro_n", 1))
            st.success(
                f"**{n_e}** lançamento(s) registrado(s)."
                if n_e > 1
                else "Lançamento registrado."
            )
        if st.session_state.pop("_ee_rec_ok", None):
            st.success("Entrada **recebida** (status Realizado); saldo de caixa atualizado.")
        st.caption(
            "**Realizado** — já entrou no caixa. **Provisionado** — expectativa (linha **Entrada prevista** no "
            "fluxo até **Receber**). **Receita Eventual / IR** em **Realizado** entram no **Dashboard** do mês. "
            "Recorrência **mensal** gera linhas com prefixo **`[REC]`**."
        )

        with st.form("form_entradas_extras", clear_on_submit=True):
            fe1, fe2 = st.columns(2)
            with fe1:
                ee_data = st.date_input(
                    "Data (prevista ou real)",
                    value=date.today(),
                    key="ee_form_data",
                )
            with fe2:
                ee_valor = st.number_input(
                    "Valor (R$)",
                    min_value=0.01,
                    value=100.0,
                    step=50.0,
                    format="%.2f",
                    key="ee_form_valor",
                )
            ee_desc = st.text_input("Descrição", placeholder="Ex.: Bônus, IR restituído", key="ee_form_desc")
            fe3, fe4 = st.columns(2)
            with fe3:
                ee_cat = st.selectbox(
                    "Categoria",
                    options=list(CATEGORIAS_ENTRADAS_EXTRAS),
                    key="ee_form_cat",
                )
            with fe4:
                ee_orig = st.text_input(
                    "Origem (opcional)", placeholder="Ex.: Conta Pessoal", key="ee_form_orig"
                )
            fe5, fe6 = st.columns(2)
            with fe5:
                ee_stat = st.selectbox(
                    "Status",
                    options=list(EE_STATUS_EXTRAS),
                    index=0,
                    key="ee_form_stat",
                    help="Provisionado não entra no saldo até Receber.",
                )
            with fe6:
                ee_rec = st.selectbox(
                    "Recorrência",
                    options=["Única", "Mensal"],
                    key="ee_form_rec",
                )
            ee_n_meses = st.number_input(
                "Quantidade de meses (só se Mensal)",
                min_value=1,
                max_value=120,
                value=12,
                step=1,
                key="ee_form_nmes",
                disabled=(ee_rec == "Única"),
            )
            if st.form_submit_button("Cadastrar", type="primary"):
                o = ee_orig.strip() or None
                if not (ee_desc or "").strip():
                    st.error("Informe a descrição.")
                else:
                    try:
                        if ee_rec == "Mensal":
                            nins = db.insert_entradas_extras_recorrentes(
                                conn,
                                data_prevista_inicial_iso=ee_data.isoformat(),
                                descricao=ee_desc.strip(),
                                valor=float(ee_valor),
                                categoria=ee_cat,
                                origem=o,
                                status=ee_stat,
                                recorrencia_mensal=True,
                                quantidade_meses=int(ee_n_meses),
                            )
                            st.session_state["_ee_cadastro_n"] = nins
                        else:
                            db.insert_entrada_extra(
                                conn,
                                data=ee_data.isoformat(),
                                descricao=ee_desc.strip(),
                                valor=float(ee_valor),
                                categoria=ee_cat,
                                origem=o,
                                status=ee_stat,
                            )
                            st.session_state["_ee_cadastro_n"] = 1
                        if "ee_table" in st.session_state:
                            del st.session_state["ee_table"]
                        st.session_state["_ee_cadastro_ok"] = True
                        st.rerun()
                    except (ValueError, sqlite3.OperationalError) as e:
                        st.error(
                            str(e)
                            if isinstance(e, ValueError)
                            else _hint_sql_erro(e)
                        )

        st.markdown("##### Receber provisões")
        st.caption("Baixa de lançamentos **Provisionado** → **Realizado** (atualiza data se informada).")
        try:
            prov_ee = db.read_sql(
                """
                SELECT id, data, descricao, valor, categoria
                FROM entradas_extras
                WHERE COALESCE(status, 'Realizado') = 'Provisionado'
                ORDER BY date(data), id
                """,
                conn,
            )
        except sqlite3.OperationalError:
            prov_ee = pd.DataFrame()
        if prov_ee.empty:
            st.caption("_Nenhuma provisão pendente._")
        else:
            for _, er in prov_ee.iterrows():
                eid = int(er["id"])
                with st.form(f"form_ee_receber_{eid}"):
                    st.write(
                        f"**{er['descricao']}** — {brl(float(er['valor']))} — data ref. "
                        f"**{str(er['data'])[:10]}**"
                    )
                    dr_rec = st.date_input(
                        "Data do recebimento",
                        value=date.today(),
                        key=f"ee_dr_{eid}",
                    )
                    if st.form_submit_button("Receber", type="primary"):
                        try:
                            db.receber_entrada_extra_provisionada(
                                conn,
                                eid,
                                data_recebimento_iso=dr_rec.isoformat(),
                            )
                            if "ee_table" in st.session_state:
                                del st.session_state["ee_table"]
                            st.session_state["_ee_rec_ok"] = True
                            st.rerun()
                        except ValueError as ex:
                            st.error(str(ex))

        st.markdown("### Lançamentos cadastrados")
        try:
            if "ee_table" not in st.session_state:
                st.session_state.ee_table = db.read_sql(
                    """
                    SELECT id, data, descricao, valor, categoria, origem, status
                    FROM entradas_extras
                    ORDER BY data DESC, id DESC
                    """,
                    conn,
                )
                if not st.session_state.ee_table.empty:
                    if "data" in st.session_state.ee_table.columns:
                        st.session_state.ee_table["data"] = pd.to_datetime(
                            st.session_state.ee_table["data"], errors="coerce"
                        ).dt.date
                    if "status" not in st.session_state.ee_table.columns:
                        st.session_state.ee_table["status"] = "Realizado"
                    else:
                        st.session_state.ee_table["status"] = st.session_state.ee_table[
                            "status"
                        ].fillna("Realizado")
            edited_ee = st.data_editor(
                st.session_state.ee_table,
                num_rows="dynamic",
                key="ee_data_editor",
                column_config={
                    "id": st.column_config.NumberColumn("ID", disabled=True, format="%d"),
                    "data": st.column_config.DateColumn("Data", format="DD/MM/YYYY"),
                    "descricao": st.column_config.TextColumn("Descrição", width="large"),
                    "valor": st.column_config.NumberColumn("Valor", format="R$ %.2f", min_value=0.01),
                    "categoria": st.column_config.SelectboxColumn(
                        "Categoria",
                        options=list(CATEGORIAS_ENTRADAS_EXTRAS),
                        required=True,
                    ),
                    "origem": st.column_config.TextColumn("Origem"),
                    "status": st.column_config.SelectboxColumn(
                        "Status",
                        options=list(EE_STATUS_EXTRAS),
                        required=True,
                    ),
                },
                hide_index=True,
                use_container_width=True,
            )
            if st.button("Salvar alterações na tabela", key="ee_btn_save"):
                _sync_entradas_extras_editor(conn, st.session_state.ee_table, edited_ee)
                _df_ee = db.read_sql(
                    """
                    SELECT id, data, descricao, valor, categoria, origem, status
                    FROM entradas_extras
                    ORDER BY data DESC, id DESC
                    """,
                    conn,
                )
                if not _df_ee.empty:
                    if "data" in _df_ee.columns:
                        _df_ee["data"] = pd.to_datetime(_df_ee["data"], errors="coerce").dt.date
                    if "status" not in _df_ee.columns:
                        _df_ee["status"] = "Realizado"
                    else:
                        _df_ee["status"] = _df_ee["status"].fillna("Realizado")
                st.session_state.ee_table = _df_ee
                st.success("Tabela atualizada.")
                st.rerun()

            with st.expander(
                "Filtro rápido — só Receita Imposto de Renda em **março**",
                expanded=False,
            ):
                st.caption(
                    "Visualização para conferência; edite os lançamentos na tabela acima. "
                    "O mês fixo é **março**; ajuste o **ano** abaixo."
                )
                ano_ir_mar = st.number_input(
                    "Ano (março)",
                    min_value=2000,
                    max_value=2100,
                    value=date.today().year,
                    step=1,
                    key="ee_filtro_ano_ir_marco",
                )
                _tdf = edited_ee
                if not _tdf.empty:
                    _dt = pd.to_datetime(_tdf["data"], errors="coerce")
                    _mask_ir = (
                        (_tdf["categoria"] == "Receita Imposto de Renda")
                        & (_dt.dt.month == 3)
                        & (_dt.dt.year == int(ano_ir_mar))
                    )
                    _filt_ir = _tdf.loc[_mask_ir].copy()
                    _tot_ir = float(_filt_ir["valor"].sum()) if not _filt_ir.empty else 0.0
                    st.metric(
                        f"Total IR em março/{int(ano_ir_mar)}",
                        brl(_tot_ir),
                    )
                    st.dataframe(
                        _filt_ir,
                        column_config={
                            "valor": st.column_config.NumberColumn(format="R$ %.2f"),
                        },
                        hide_index=True,
                        use_container_width=True,
                    )
                    if _filt_ir.empty:
                        st.caption("_Nenhuma linha de Imposto de Renda em março para este ano._")
                else:
                    st.caption("_Tabela vazia — cadastre lançamentos acima._")
        except sqlite3.OperationalError as exc:
            st.error(
                "Não foi possível ler `entradas_extras`. "
                + _hint_sql_erro(exc).replace("dividas_emprestimos", "entradas_extras")
            )

    with tab_flux:
        st.subheader("Fluxo de Caixa Projetado")
        st.caption(
            "**Diário:** honorários no **dia_vencimento** de cada cliente (vigência respeitada). "
            "**Saídas realizadas:** débito em conta (`transacoes`). **Saídas previstas (tracejado):** "
            "faturas de cartão não pagas + **provisões** da aba Despesas. "
            "**Entrada prevista (extras):** linha **azul pontilhada** — `Entradas Extras` com status **Provisionado** até **Receber**. "
            "**Entradas extras realizadas** permanecem nas barras azul claro."
        )

        ref_v = st.date_input(
            "Referência para próximos vencimentos",
            value=date.today(),
            key="fluxo_ref_venc",
        )
        saldo_hoje = db.saldo_caixa_total(conn)

        r_cfg1, r_cfg2 = st.columns(2)
        with r_cfg1:
            fluxo_ini = st.date_input(
                "Início da projeção",
                value=date.today(),
                key="fluxo_data_ini",
            )
        with r_cfg2:
            n_dias_proj = st.number_input(
                "Quantidade de dias",
                min_value=7,
                max_value=120,
                value=45,
                step=1,
                key="fluxo_n_dias",
            )

        fluxo_df, saldo0, data_pior = build_fluxo_projetado(
            conn, fluxo_ini, int(n_dias_proj)
        )
        pior_valor = (
            float(fluxo_df["saldo_projetado"].min())
            if not fluxo_df.empty
            else float(saldo0)
        )

        k0, k1, k2, k3 = st.columns(4)
        with k0:
            st.metric(
                "Saldo atual (caixa)",
                brl(saldo_hoje),
                help="Transações **realizadas** + entradas extras (base da linha de saldo projetado).",
            )
        with k1:
            st.metric(
                "Pior saldo projetado",
                brl(pior_valor),
                help="Menor saldo acumulado ao longo do período (maior aperto de caixa).",
            )
        with k2:
            dp = (
                data_pior.strftime("%d/%m/%Y")
                if data_pior is not None
                else "—"
            )
            st.metric("Dia do maior aperto", dp)
        with k3:
            st.metric(
                "Saldo inicial (base)",
                brl(saldo0),
                help="Só transações **realizadas** + entradas extras, até antes do 1º dia da projeção.",
            )

        col_ven = st.container()
        with col_ven:
            st.markdown("**Próximos vencimentos**")
            st.markdown(upcoming_events_md(conn, ref_v))

        if fluxo_df.empty:
            st.warning("Nenhum dia gerado para a projeção.")
        else:
            fig = fig_fluxo_diario(fluxo_df)
            st.plotly_chart(fig, use_container_width=True)

            cols_tab = [
                "dia",
                "receitas_servico",
                "receitas_extras",
                "receitas_extras_previstas",
                "receitas",
                "despesas_real",
                "despesas_previstas",
                "despesas",
                "saldo_projetado",
            ]
            tab_view = fluxo_df[[c for c in cols_tab if c in fluxo_df.columns]].copy()
            rename = {
                "dia": "Dia",
                "receitas_servico": "Receita (serviço)",
                "receitas_extras": "Entradas extras (real)",
                "receitas_extras_previstas": "Entrada prevista (extras)",
                "receitas": "Receita total",
                "despesas_real": "Desp. realizada",
                "despesas_previstas": "Desp. prevista",
                "despesas": "Despesa total",
                "saldo_projetado": "Saldo projetado",
            }
            tab_view = tab_view.rename(columns=rename)
            st.markdown("**Tabela de apoio**")
            st.dataframe(
                tab_view,
                column_config={
                    "Receita (serviço)": st.column_config.NumberColumn(format="R$ %.2f"),
                    "Entradas extras (real)": st.column_config.NumberColumn(format="R$ %.2f"),
                    "Entrada prevista (extras)": st.column_config.NumberColumn(format="R$ %.2f"),
                    "Receita total": st.column_config.NumberColumn(format="R$ %.2f"),
                    "Desp. realizada": st.column_config.NumberColumn(format="R$ %.2f"),
                    "Desp. prevista": st.column_config.NumberColumn(format="R$ %.2f"),
                    "Despesa total": st.column_config.NumberColumn(format="R$ %.2f"),
                    "Saldo projetado": st.column_config.NumberColumn(format="R$ %.2f"),
                },
                hide_index=True,
                use_container_width=True,
            )

        st.caption(
            f"Projeção linear a partir de **{fluxo_ini.strftime('%d/%m/%Y')}** por "
            f"**{int(n_dias_proj)}** dias. Saldo da linha inicia em **{brl(saldo0)}** "
            f"(transações + entradas extras)."
        )

    with tab_rp:
        st.subheader("Realizado vs. Projetado")
        st.caption(
            "**Realizado:** entradas/saídas em `transacoes` por mês + **entradas extras** (azul no gráfico). "
            "**Var. receitas** compara receita de **serviço** com honorários previstos. "
            "**Previsto (despesas):** estimativa de faturas dos cartões + provisões não realizadas no mês. "
            "**Dívidas** não entram no previsto — apenas monitoramento na aba Dívidas."
        )
        hoje = date.today()
        default_ini = _sub_months(date(hoje.year, hoje.month, 1), 11)
        rp_c1, rp_c2 = st.columns(2)
        with rp_c1:
            mes_inicio_rp = st.date_input(
                "Primeiro mês da série",
                value=default_ini,
                key="rp_mes_inicio",
            )
        with rp_c2:
            n_meses_rp = st.number_input(
                "Quantidade de meses",
                min_value=1,
                max_value=60,
                value=12,
                step=1,
                key="rp_n_meses",
            )
        mes_ini = date(mes_inicio_rp.year, mes_inicio_rp.month, 1)
        df_rp = build_realizado_previsto_df(conn, mes_ini, int(n_meses_rp))
        if df_rp.empty:
            st.warning("Nenhum mês na série.")
        else:
            st.plotly_chart(
                fig_realizado_previsto(df_rp),
                use_container_width=True,
            )
            st.markdown("**Variação (Real − Previsto)**")
            tab_rp_view = df_rp.drop(columns=["ym"], errors="ignore")
            st.dataframe(
                _style_realizado_previsto(tab_rp_view),
                use_container_width=True,
                hide_index=True,
            )


if __name__ == "__main__":
    main()
