# -*- coding: utf-8 -*-
from thriftpy.thrift import TClient
from thriftpy.thrift import TApplicationException
from thriftpy.transport import TTransportException
from socket import error as socket_error
from collections import deque
from time import sleep, time
import logging
import numpy as np

logger = logging.getLogger(__name__)


class RecoveringClient(TClient):

    _log_interval = 60.0

    def __init__(self, *args, **kwargs):
        self._connection = kwargs.pop("connection", None)
        self._retries = kwargs.pop("retries", (0, 5, 30))
        self._resp_times = deque(maxlen=50)
        self._next_log_time = time() + self._log_interval
        super(RecoveringClient, self).__init__(*args, **kwargs)

    def _req(self, _api, *args, **kwargs):
        no_retry = kwargs.pop("no_retry", False)
        retries = deque(self._retries)
        interval = 0
        client = super(RecoveringClient, self)
        while True:
            t = time()
            try:
                return client._req(_api, *args, **kwargs)
            except (TApplicationException, socket_error, TTransportException) as exc:
                logger.exception("Got exception")
                while True:
                    interval = retries.popleft() if retries else interval
                    logger.info("Sleeping for %d seconds", interval)
                    sleep(interval)
                    logger.info("Trying to reconnect")
                    try:
                        self._connection._refresh_thrift_client()
                        self._connection.open()
                        client = super(RecoveringClient, self._connection.client)
                        logger.debug("New client is initialized")
                    except TTransportException:
                        logger.exception("Got exception, while trying to reconnect. Continuing")
                        pass
                    else:
                        break
                if no_retry:
                    raise exc
            finally:
                now = time()
                elapsed = (now - t) * 1E+3
                logger.debug("RPC call to %s took %d ms", _api, elapsed)
                self._resp_times.append(elapsed)
                if self._next_log_time < now:
                    self._log_response_times()
                    self._next_log_time = now + self._log_interval

    def _log_response_times(self):
        logger.debug("client id %d response times (ms) statistics", id(self))
        logger.debug("mean: %f stdev: %f median: %f count: %d",
                     np.mean(self._resp_times),
                     np.std(self._resp_times),
                     np.median(self._resp_times),
                     len(self._resp_times))