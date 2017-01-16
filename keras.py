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
    def __init__(self, print_every=1000000,
                 start=None, counter=None, metrics=None):
        self._print_every = print_every
        self._partial_counter = 0
        self._total_counter = counter if counter is not None else 0
        self._start = start if start is not None else timer()
        self._metrics = ['loss'] if metrics is None else list(metrics)

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

        if self._partial_counter >= self._print_every:
            delta = timer() - self._start
            metrics = ' - '.join(
                '{}: {:.1e}'.format(m, float(logs.get(m)))
                for m in self._metrics if m in logs
            )

            print(
                '[keras] {:,} trained in {} ({:.1e} s/example) – {}'.format(
                    self._total_counter, time_formatter(delta),
                    self._total_counter / delta, metrics
                )
            )
            self._partial_counter = 0


def plot_weights(model, dest_dir, layers=None, dead_threshold=1e-8):
    for layer in model.layers:
        if layers is not None and layer not in layers:
            continue

        try:
            W, b = model.get_layer(layer.name).get_weights()
        except ValueError:
            # this is not a model with weights (e.g. noise layer)
            continue

        dead_connections = sum(
            1 if w < dead_threshold else 0 for w in numpy.nditer(W)
        )
        dead_connections_percent = dead_connections / numpy.prod(W.shape)

        print('[analysis] layer {}: {:,} dead connections ({:.1%})'.format(
            layer.name, dead_connections, dead_connections_percent
        ))

        cmap_range = max(
            numpy.max(W), numpy.abs(numpy.min(W)),
            numpy.max(b), numpy.abs(numpy.min(b)),
        )

        b = b.reshape(b.shape[0], 1) @ numpy.ones((1, 10))

        fig, (ax1, ax2) = plt.subplots(1, 2, sharey=False)

        fig.suptitle('Weights {}'.format(layer.name))

        ax1.imshow(
            W, cmap='coolwarm', interpolation='nearest',
            vmin=-cmap_range, vmax=cmap_range
        )
        cax = ax2.imshow(
            b, cmap='coolwarm', interpolation='nearest',
            vmin=-cmap_range, vmax=cmap_range
        )
        ax2.set_xticklabels([])

        cbar = fig.colorbar(cax, ticks=[-cmap_range, 0, cmap_range])
        cbar.ax.set_yticklabels(
            ['{:.1e}'.format(-cmap_range), ' 0', '{:.1e}'.format(cmap_range)]
        )

        output_fn = os.path.join(
            dest_dir, '{}-{}.pdf'.format(model.name, layer.name)
        )
        fig.savefig(output_fn)
        fig.clf()

