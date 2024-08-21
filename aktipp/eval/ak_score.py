import numpy as np
from numpy.typing import ArrayLike


def ak_score(y_true: ArrayLike, y_pred: ArrayLike) -> np.ndarray:
    """Calculate the score based on 2 points for the correct tendency, another two
    points for the corret goal difference (only for decisive games) and finally
    another three points for the correct result.

    Parameters
    ----------
    y_true: np.ArrayLike
        ArrayLike with two columns containing the values for goals_team_1 and
        goals_team_2.
    y_pred: np.ArrayLike
        ArrayLike with two columns containing the values for goals_team_1_pred and
        goals_team_2_pred.

    Returns
    -------
    ak_score: np.ndarray
        Calculated score for every game.
    """
    y_true = np.array(y_true, dtype=np.int64)
    y_pred = np.array(y_pred, dtype=np.int64)

    ak_score = np.where(
        (y_true[..., 0] == y_pred[..., 0])
        & (y_true[..., 1] == y_pred[..., 1]),  # exact result
        np.where(
            y_true[..., 0] != y_true[..., 1],
            7,  # exact result decisive game
            5,  # exact result drawn game
        ),
        np.where(
            (y_true[..., 0] - y_true[..., 1] == y_pred[..., 0] - y_pred[..., 1])
            & (y_true[..., 0] != y_true[..., 1]),
            4,  # goal diff decisive game
            np.where(
                (
                    (y_true[..., 0] - y_true[..., 1] > 0)
                    & (y_pred[..., 0] - y_pred[..., 1] > 0)
                )  # win team 1
                | (
                    (y_true[..., 0] - y_true[..., 1] == 0)
                    & (y_pred[..., 0] - y_pred[..., 1] == 0)
                )  # draw
                | (
                    (y_true[..., 0] - y_true[..., 1] < 0)
                    & (y_pred[..., 0] - y_pred[..., 1] < 0)
                ),  # win team 2
                2,
                0,
            ),
        ),
    )

    return ak_score


def mean_ak_score(y_true: ArrayLike, y_pred: ArrayLike):
    """Mean ak_score.

    Parameters
    ----------
    y_true: np.ArrayLike
        ArrayLike with two columns containing the values for goals_team_1 and
        goals_team_2.
    y_pred: np.ArrayLike
        ArrayLike with two columns containing the values for goals_team_1_pred and
        goals_team_2_pred.

    Returns
    -------
    mean_ak_score: np.ndarray
        Mean ak_score.
    """
    return np.mean(ak_score(y_true, y_pred))
