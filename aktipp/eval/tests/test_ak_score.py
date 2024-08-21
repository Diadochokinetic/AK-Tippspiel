from numpy.testing import assert_array_equal
from aktipp.eval import ak_score


def test_ak_score():
    y_true = [
        [1, 0],  # exact result team 1 win
        [1, 1],  # exact result draw
        [0, 1],  # exact result team 2 win
        [2, 1],  # correct goal diff team 1 win
        [2, 2],  # correct goal diff draw (equals correct tendency)
        [1, 2],  # correct goal diff team 2 win
        [2, 1],  # correct tendency team 1 win
        [1, 2],  # correct tendency team 2 win
        [1, 0],  # wrong team wins
        [0, 0],  # wrong draw
    ]

    y_pred = [
        [1, 0],  # exact result team 1 win
        [1, 1],  # exact result draw
        [0, 1],  # exact result team 2 win
        [1, 0],  # correct goal diff team 1 win
        [0, 0],  # correct goal diff draw (equals correct tendency)
        [0, 1],  # correct goal diff team 2 win
        [5, 1],  # correct tendency team 1 win
        [1, 5],  # correct tendency team 2 win
        [0, 1],  # wrong team wins
        [1, 0],  # wrong draw
    ]

    ak_score_true = [
        7,  # exact result team 1 win
        5,  # exact result draw
        7,  # exact result team 2 win
        4,  # correct goal diff team 1 win
        2,  # correct goal diff draw (equals correct tendency)
        4,  # correct goal diff team 2 win
        2,  # correct tendency team 1 win
        2,  # correct tendency team 2 win
        0,  # wrong team wins
        0,  # wrong draw
    ]

    ak_score_test = ak_score(y_true, y_pred)

    assert_array_equal(ak_score_true, ak_score_test)
