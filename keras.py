# built-in modules
import os
from itertools import chain

# installed modules
import numpy
from keras.callbacks import Callback
import keras.backend as K
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
        self._metrics = {m: [] for m in (metrics if metrics else {})}
        super(NotSoChattyLogger, self).__init__()

    @property
    def start(self):
        return self._start

    def __getitem__(self, item):
        return self._metrics[item]

    @property
    def count(self):
        return self._total_counter

    def on_batch_end(self, batch, logs={}):
        self._partial_counter += logs.get('size', 0)
        self._total_counter += logs.get('size', 0)

        if self._partial_counter >= self._print_every:
            [
                self._metrics[k].append(v)
                for k, v in logs.items() if k in self._metrics
            ]

            current_logs = {
                k: v[-1] for k, v in sorted(self._metrics.items()) if v
            }

            delta = timer() - self._start
            metrics = ' - '.join(
                '{}: {:.1e}'.format(m, float(v))
                for m, v in current_logs.items()
            )

            print(
                '[keras] {:,} trained in {} ({:.1e} s/example) – {}'.format(
                    self._total_counter, time_formatter(delta),
                    delta / self._total_counter, metrics
                )
            )
            self._partial_counter = 0


def plot_weights(model, dest_dir, layers=None):
    for layer in model.layers:
        if layers is not None and layer not in layers:
            continue

        try:
            W, b = model.get_layer(layer.name).get_weights()
        except ValueError:
            # this is not a model with weights (e.g. noise layer)
            continue

        dead_connections = sum(
            1 if w < K.epsilon() else 0 for w in numpy.nditer(W)
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

        fig.suptitle(
            'Weights {}'.format(layer.name), fontsize=16, fontweight='bold'
        )

        cax1 = ax1.imshow(
            W, cmap='coolwarm', interpolation='nearest',
            vmin=-cmap_range, vmax=cmap_range, origin='lower'
        )
        x, y = W.shape
        ax1.set_ylim([0, x])
        ax1.set_xlim([0, y])
        ax1.set_title('Weights')
        ax1.set_ylabel('input')
        ax1.set_xlabel('output')

        for t in chain(ax1.xaxis.get_ticklines(), ax1.yaxis.get_ticklines()):
            t.set_visible(False)

        ax2.imshow(
            b, cmap='coolwarm', interpolation='nearest',
            vmin=-cmap_range, vmax=cmap_range, origin='lower'
        )
        ax2.set_title('Bias')
        ax2.set_xticks([])
        for t in chain(ax2.xaxis.get_ticklines(), ax2.yaxis.get_ticklines()):
            t.set_visible(False)
        ax2.set_ylabel('output')

        cbar = fig.colorbar(cax1, ticks=[-cmap_range, 0, cmap_range])
        cbar.ax.set_yticklabels(
            ['{:.1e}'.format(-cmap_range), ' 0', '{:.1e}'.format(cmap_range)]
        )

        output_fn = os.path.join(
            dest_dir, '{}-{}.pdf'.format(model.name, layer.name)
        )
        fig.savefig(output_fn)
        fig.clf()


def remove_shape_check_batch_input_output(model):
    """Remove shape check between input and output in training
    when training the model"""

    def custom_standardize_user_data(
            x, y, sample_weight=None, class_weight=None,
            check_batch_axis=True, batch_size=None
    ):
        if not hasattr(model, 'optimizer'):
            raise RuntimeError('You must compile a model before '
                               'training/testing. '
                               'Use `model.compile(optimizer, loss)`.')

        output_shapes = []
        for output_shape, loss_fn in \
                zip(model._feed_output_shapes, model._feed_loss_fns):
            if loss_fn.__name__ == 'sparse_categorical_crossentropy':
                output_shapes.append(output_shape[:-1] + (1,))
            elif getattr(keras.losses, loss_fn.__name__, None) is None:
                output_shapes.append(None)
            else:
                output_shapes.append(output_shape)

        x = keras.engine.training._standardize_input_data(
            x, model._feed_input_names, model._feed_input_shapes,
            check_batch_axis=False, exception_prefix='model input'
        )

        y = keras.engine.training._standardize_input_data(
            y, model._feed_output_names, output_shapes,
            check_batch_axis=False, exception_prefix='model target'
        )

        sample_weights = keras.engine.training._standardize_sample_weights(
            sample_weight, model._feed_output_names
        )

        class_weights = keras.engine.training._standardize_class_weights(
            class_weight, model._feed_output_names
        )

        sample_weights = [
            keras.engine.training._standardize_weights(ref, sw, cw, mode)
            for (ref, sw, cw, mode) in zip(
                y, sample_weights, class_weights,
                model._feed_sample_weight_modes
            )
        ]

        # keras.engine.training._check_array_lengths(x, y, sample_weights)

        keras.engine.training._check_loss_and_target_compatibility(
            y, model._feed_loss_fns, model._feed_output_shapes
        )

        if model.stateful and batch_size:
            if x[0].shape[0] % batch_size != 0:
                raise ValueError('In a stateful network, '
                                 'you should only pass inputs with '
                                 'a number of samples that can be '
                                 'divided by the batch size. Found: ' +
                                 str(x[0].shape[0]) + ' samples')
        return x, y, sample_weights

    model._standardize_user_data, model._def_standardize_user_data =\
        custom_standardize_user_data, model._standardize_user_data
