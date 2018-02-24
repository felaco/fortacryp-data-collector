from keras.models import Sequential
from keras.layers import Dense, Dropout
from keras.optimizers import Adam, RMSprop, SGD
from keras.callbacks import Callback
import sys
import keras

class BestModelCallback(Callback):
    def on_train_begin(self, logs=None):
        self.best_loss = sys.float_info.max
        self.best_model = None

    def on_epoch_end(self, epoch, logs=None):
        test_loss = logs.get('val_loss')
        if test_loss < self.best_loss:
            self.best_loss = test_loss
            self.best_model = keras.models.clone_model(self.model)


def simple_model(n_inputs=1, n_outputs=1):
    model = Sequential()
    model.add(Dense(units=20, activation='relu', input_dim=n_inputs))
    model.add(Dense(units=30, activation='relu'))
    model.add(Dense(units=n_outputs, activation='relu'))

    model.compile(optimizer='adagrad', loss='mse')
    return model