from celery import shared_task
from celery.utils.log import get_task_logger

from django.dispatch.dispatcher import (
    _make_id,
    Signal,
)

from .tasks import propagate_signal, call_receiver


class AsyncSignal(Signal):

    def __init__(self, providing_args=None, queue=None):

        super(AsyncSignal, self).__init__(providing_args=providing_args)
        self.queue = queue

    def send(self, sender, **named):
        """Send the signal via Celery."""

        for receiver in self._live_receivers(_make_id(sender)):
        try:
            logger.info("START Receiver: {}; Signal: {}; sender: {}, kwargs:{}".format(receiver,signal,sender,named))
            call_receiver.apply_async(
                args=(receiver, self, sender,),
                kwargs=named,
                queue=self.queue,
            )
            logger.info("END Receiver: {}; Signal: {}; sender: {}, kwargs:{}".format(receiver,signal,sender,named))
        except Exception as ex:
            logger.info("EXCEPT START Receiver: {}; Signal: {}; sender: {}, kwargs:{}".format(receiver,signal,sender,named))
            logger.error(ex)
            logger.info("EXCEPT END Receiver: {}; Signal: {}; sender: {}, kwargs:{}".format(receiver,signal,sender,named))
    
        
#         propagate_signal.apply_async(
#             args=(self, sender,),
#             kwargs=named,
#             queue=self.queue,
#         )
