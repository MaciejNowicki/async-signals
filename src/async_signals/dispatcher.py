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

        logger = get_task_logger(__name__)
        logger.info("START send")
        logger.info(self)
        logger.info(sender)
        logger.info(_make_id(sender))
        logger.info(self._live_receivers(sender))
        
        for receiver in self._live_receivers(sender):
            try:
                logger.info("START Receiver: {}; Signal: {}; sender: {}, kwargs:{}".format(receiver,self,sender,named))
                call_receiver.apply_async(
                    args=(receiver, self, sender,),
                    kwargs=named,
                    queue=self.queue,
                )
                logger.info("END Receiver: {}; Signal: {}; sender: {}, kwargs:{}".format(receiver,self,sender,named))
            except Exception as ex:
                logger.info("EXCEPT START Receiver: {}; Signal: {}; sender: {}, kwargs:{}".format(receiver,self,sender,named))
                logger.error(ex)
                logger.info("EXCEPT END Receiver: {}; Signal: {}; sender: {}, kwargs:{}".format(receiver,self,sender,named))
    
        logger.info("END send")
        
#         propagate_signal.apply_async(
#             args=(self, sender,),
#             kwargs=named,
#             queue=self.queue,
#         )
