import numpy as np
from sklearn.metrics import mean_poisson_deviance


def multi_mean_poisson_deviance(y_true, y_pred):
    y_true = np.array(y_true, dtype=np.float64)
    y_pred = np.array(y_pred, dtype=np.float64)

    n_targets = y_true.shape[1]
    if n_targets == 1:
        return mean_poisson_deviance(y_true, y_pred)
    else:
        return np.mean(
            [
                mean_poisson_deviance(y_true[..., i], y_pred[..., i])
                for i in range(n_targets)
            ]
        )
