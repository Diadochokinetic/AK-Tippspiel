import polars as pl


def _suffix_alias(cols: str | list[str], suffix: str) -> pl.Expr | list[pl.Expr]:
    if isinstance(cols, str):
        return pl.col(cols).alias(f"{cols}{suffix}")
    elif isinstance(cols, list[str]):
        expressions = []
        for col in cols:
            expressions.append(pl.col(col).alias(f"{col}{suffix}"))
        return expressions
    else:
        raise ValueError("cols need to be either a str or list[str].")
