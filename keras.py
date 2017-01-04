# installed modules
import numpy
from keras.engine import Layer
from keras.callbacks import Callback
import keras.backend as K

# project modules
from .meta import time_formatter, timer


class NotSoChattyLogger(Callback):
    def __init__(self, print_every=1000000, previous_losses=None):
        self._print_every = print_every
        self._partial_counter = 0
        self._total_counter = 0
        self.losses = previous_losses if previous_losses else []
        self._start = timer()

        super(NotSoChattyLogger, self).__init__()

    def on_batch_end(self, batch, logs={}):
        self._partial_counter += logs.get('size', 0)
        self._total_counter += logs.get('size', 0)

        loss = float(logs.get('loss'))

        if self._partial_counter >= self._print_every:

            delta = timer() - self._start

            print(
                '[keras] {:,} trained in {} ({:.1e} s/example) '
                '– loss: {:.1e}'.format(
                    self._total_counter, time_formatter(delta),
                    self._total_counter / delta, loss
                )
            )
            self._partial_counter = 0

        self.losses.append(loss)
