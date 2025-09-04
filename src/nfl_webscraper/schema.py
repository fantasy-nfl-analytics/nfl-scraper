"""Schema harmonization utilities."""

from __future__ import annotations

import polars as pl


def _collect_all_columns(frames: list[pl.DataFrame]) -> list[str]:
	"""Collect all unique columns and order them with year/category first."""
	all_cols: set[str] = set()
	for f in frames:
		all_cols.update(f.columns)
	return [c for c in ['year', 'category'] if c in all_cols] + [
		c for c in sorted(all_cols) if c not in {'year', 'category'}
	]


def _build_target_schema(frames: list[pl.DataFrame]) -> dict[str, pl.datatypes.PolarsDataType]:
	"""Build target schema from all frames, preferring non-Null types."""
	target: dict[str, pl.datatypes.PolarsDataType] = {}
	for f in frames:
		for col, dt in zip(f.columns, f.dtypes, strict=False):
			if col not in target and dt != pl.Null:
				target[col] = dt
	target.setdefault('year', pl.Int64)
	target.setdefault('category', pl.Utf8)
	return target


def _normalize_frame(
	frame: pl.DataFrame,
	ordered_cols: list[str],
	target_schema: dict[str, pl.datatypes.PolarsDataType]
) -> pl.DataFrame:
	"""Add missing columns and cast types to match target schema."""
	# Add missing columns
	adds = []
	for c in ordered_cols:
		if c not in frame.columns:
			lit = pl.lit(None).alias(c)
			if target_schema.get(c) and target_schema[c] != pl.Null:
				lit = lit.cast(target_schema[c])
			adds.append(lit)
	if adds:
		frame = frame.with_columns(adds)

	# Cast null columns to target types
	casts = []
	for col, dt in zip(frame.columns, frame.dtypes, strict=False):
		tgt = target_schema.get(col)
		if tgt and dt == pl.Null and tgt != pl.Null:
			casts.append(pl.col(col).cast(tgt))
	if casts:
		frame = frame.with_columns(casts)

	return frame.select(ordered_cols)


def unify_frames(frames: list[pl.DataFrame]) -> pl.DataFrame:
	"""Unify multiple DataFrames with potentially different schemas."""
	if not frames:
		return pl.DataFrame([])

	ordered_cols = _collect_all_columns(frames)
	target_schema = _build_target_schema(frames)

	unified_frames = [
		_normalize_frame(f, ordered_cols, target_schema)
		for f in frames
	]

	if not unified_frames:
		return pl.DataFrame([])

	result = unified_frames[0]
	for frame in unified_frames[1:]:
		result = result.vstack(frame, in_place=False)

	return result


__all__ = ['unify_frames']
