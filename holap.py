import numpy as np


def eval_model(model, last_observation, times=1):
    y_pred_list = []
    x = last_observation
    def predict(model, val, list):
        y = model.predict(val)
        list.append(y)

    predict(model, x, y_pred_list)
    for i in range(times):
        # si i es 5; el vector x con valores [1,2,3,4] se convierte en [5,1,2,3]
        x = np.append(i, x[:-1])
        predict(model, x, y_pred_list)

    return np.array(y_pred_list)
