from celery import shared_task
from celery.utils.log import get_task_logger

from django.dispatch.dispatcher import (
    _make_id,
    Signal,
)

from .tasks import propagate_signal


class AsyncSignal(Signal):

    def __init__(self, providing_args=None, queue=None):

        super(AsyncSignal, self).__init__(providing_args=providing_args)
        self.queue = queue

    def send(self, sender, **named):
        """Send the signal via Celery."""

        propagate_signal.apply_async(
            args=(self, sender,),
            kwargs=named,
            queue=self.queue,
        )
