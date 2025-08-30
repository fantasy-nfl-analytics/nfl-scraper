"""Schema harmonization utilities."""
from __future__ import annotations
import polars as pl

def unify_frames(frames: list[pl.DataFrame]) -> pl.DataFrame:
    if not frames:
        return pl.DataFrame([])
    all_cols: set[str] = set()
    for f in frames:
        all_cols.update(f.columns)
    ordered = [c for c in ["year","category"] if c in all_cols] + [c for c in sorted(all_cols) if c not in {"year","category"}]
    target: dict[str, pl.datatypes.PolarsDataType] = {}
    for f in frames:
        for col, dt in zip(f.columns, f.dtypes):
            if col not in target and dt != pl.Null:
                target[col] = dt
    target.setdefault("year", pl.Int64)
    target.setdefault("category", pl.Utf8)
    out: pl.DataFrame | None = None
    for f in frames:
        adds = []
        for c in ordered:
            if c not in f.columns:
                lit = pl.lit(None).alias(c)
                if target.get(c) and target[c] != pl.Null:
                    lit = lit.cast(target[c])
                adds.append(lit)
        if adds:
            f = f.with_columns(adds)
        casts = []
        for col, dt in zip(f.columns, f.dtypes):
            tgt = target.get(col)
            if tgt and dt == pl.Null and tgt != pl.Null:
                casts.append(pl.col(col).cast(tgt))
        if casts:
            f = f.with_columns(casts)
        f = f.select(ordered)
        out = f if out is None else out.vstack(f, in_place=False)
    return out if out is not None else pl.DataFrame([])

__all__ = ["unify_frames"]
