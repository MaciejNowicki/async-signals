from celery import shared_task
from celery.utils.log import get_task_logger

from django.dispatch.dispatcher import (
    _make_id,
    Signal,
)


@shared_task
def propagate_signal(self, sender, **named):
    """
    Send signal from sender to all connected receivers catching errors.

    Arguments:

            sender The sender of the signal. Can be any python object
                (normally one registered with a connect if you
                actually want something to occur).

            named
                Named arguments which will be passed to receivers. These
                arguments must be a subset of the argument names defined in
                providing_args.

    Return a list of tuple pairs [(receiver, response), ... ]. May raise
    DispatcherKeyError.

    If any receiver raises an error (specifically any subclass of
    Exception), the error instance is returned as the result for that
    receiver.
    """

    logger = get_task_logger(__name__)
    logger.info("START propagate_signal")
    logger.info(self)
    logger.info(sender)
    logger.info(_make_id(sender))
    logger.info(self._live_receivers(_make_id(sender)))
    
    # Call each receiver with whatever arguments it can accept.
    for receiver in self._live_receivers(_make_id(sender)):
        try:
            logger.info("START Receiver: {}; Signal: {}; sender: {}, kwargs:{}".format(receiver,signal,sender,named))
            receiver(signal=self, sender=sender, **named)
            logger.info("END Receiver: {}; Signal: {}; sender: {}, kwargs:{}".format(receiver,signal,sender,named))
        except Exception as ex:
            logger.info("EXCEPT START Receiver: {}; Signal: {}; sender: {}, kwargs:{}".format(receiver,signal,sender,named))
            logger.error(ex)
            logger.info("EXCEPT END Receiver: {}; Signal: {}; sender: {}, kwargs:{}".format(receiver,signal,sender,named))
    
    logger.info("END propagate_signal")


@shared_task
def call_receiver(receiver, self, sender, **named):
    receiver(signal=self, sender=sender, **named)
    
