import polars as pl
from sklearn.base import BaseEstimator


class DummyClassifier(BaseEstimator):
    def __init__(self): ...

    def fit(self, X, y):
        pass

    def predict(self, X):
        return X.select(
            pl.when(pl.col("home_team") == 1)
            .then(1)
            .otherwise(-1)
            .alias("result_class_pred"),
        )


class GoalsMapper:
    def __init__(
        self,
        estimator: BaseEstimator,
        suffix: str | None = None,
        threshold=0.5,
    ):
        self.estimator = estimator
        self.suffix = suffix

    def _calc_exact_result(self):
        return (
            pl.col("goals_team_1").cast(pl.String)
            + pl.lit(":")
            + pl.col("goals_team_2").cast(pl.String)
        )

    def _map_result_class(self):
        return pl.col("result_class_pred").replace_strict(
            self.result_class_map, default=self.fallback_exact_result
        )

    def _unpack_goals_pred(self):
        return (
            pl.col("exact_result_pred")
            .str.split(":")
            .list.to_struct(fields=["goals_team_1_pred", "goals_team_2_pred"])
        )

    def fit(self, X, y, goals):
        self.estimator.fit(X, y)

        df = pl.concat([goals, y], how="horizontal").with_columns(
            exact_result=self._calc_exact_result()
        )

        self.result_class_map = dict(
            df.group_by("result_class")
            .agg(pl.col("exact_result").mode())
            .explode("exact_result")
            .iter_rows()
        )
        self.fallback_exact_result = df["exact_result"].mode()

    def predict(self, X):
        y_pred = (
            pl.DataFrame(self.estimator.predict(X), schema=["result_class_pred"])
            .with_columns(exact_result_pred=self._map_result_class())
            .with_columns(goals_pred_struct=self._unpack_goals_pred())
            .unnest("goals_pred_struct")
            .with_columns(
                pl.col("goals_team_1_pred").cast(pl.Int64),
                pl.col("goals_team_2_pred").cast(pl.Int64),
            )
        )

        if self.suffix is not None:
            y_pred.columns = [col + self.suffix for col in y_pred.columns]

        return y_pred
