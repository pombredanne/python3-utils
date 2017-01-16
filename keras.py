# built-in modules
import os

# installed modules
import numpy
from keras.engine import Layer
from keras.callbacks import Callback
import keras.backend as K
import h5py
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# project modules
from .meta import time_formatter, timer


class NotSoChattyLogger(Callback):
    def __init__(self, print_every=1000000, previous_losses=None,
                 start=None, counter=None):
        self._print_every = print_every
        self._partial_counter = 0
        self._total_counter = counter if counter is not None else 0
        self.losses = previous_losses if previous_losses else []
        self._start = start if start is not None else timer()

        super(NotSoChattyLogger, self).__init__()

    @property
    def start(self):
        return self._start

    @property
    def count(self):
        return self._total_counter

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


def plot_weights(model, dest_dir, layers=None):
    for layer in model.layers:
        if layers is not None and layer not in layers:
            continue

        try:
            W, b = model.get_layer(layer.name).get_weights()
        except ValueError:
            # this is not a model with weights (e.g. noise layer)
            continue

        range = max(
            numpy.max(W), numpy.abs(numpy.min(W)),
            numpy.max(b), numpy.abs(numpy.min(b)),
        )

        b = b.reshape(b.shape[0], 1) @ numpy.ones((1, 10))

        fig, (ax1, ax2) = plt.subplots(1, 2, sharey=False)

        fig.suptitle('Weights {}'.format(layer.name))

        ax1.imshow(W, cmap='coolwarm', vmin=-range, vmax=range, interpolation='none')
        cax = ax2.imshow(b, cmap='coolwarm', vmin=-range, vmax=range, interpolation='none')

        cbar = fig.colorbar(cax, ticks=[-range, 0, range])
        cbar.ax.set_yticklabels(['{:.1e}'.format(-range), ' 0', '{:.1e}'.format(range)])

        output_fn = os.path.join(
            dest_dir, '{}-{}.pdf'.format(model.name, layer.name)
        )
        fig.savefig(output_fn)
        fig.clf()

