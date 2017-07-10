# -*- coding: utf-8 -*-

import time
import logging
import datetime
from functools import partial
from socket import error as socket_error
from collections import deque, defaultdict

from thriftpy.thrift import TClient
from thriftpy.thrift import TApplicationException
from thriftpy.transport import TTransportException


TIMINGS_PER_API_KEEP_LIMIT = 200
EPOCH_DATETIME = datetime.datetime.utcfromtimestamp(0)

logger = logging.getLogger(__name__)


def utc_timestamp():
    return (datetime.datetime.utcnow() - EPOCH_DATETIME).total_seconds()


class RecoveringClient(TClient):

    def __init__(self, *args, **kwargs):
        self._connection = kwargs.pop("connection", None)
        self._retries = kwargs.pop("retries", (0, 5, 30))
        # a dict with deque per api entry call
        self._response_timings = defaultdict(partial(
            deque, maxlen=TIMINGS_PER_API_KEEP_LIMIT))
        super(RecoveringClient, self).__init__(*args, **kwargs)

    def _req(self, _api, *args, **kwargs):
        no_retry = kwargs.pop("no_retry", False)
        retries = deque(self._retries)
        interval = 0
        client = super(RecoveringClient, self)
        while True:
            t = utc_timestamp()
            try:
                return client._req(_api, *args, **kwargs)
            except (TApplicationException, socket_error, TTransportException) as exc:
                logger.exception("Got exception")
                while True:
                    interval = retries.popleft() if retries else interval
                    logger.info("Sleeping for %d seconds", interval)
                    time.sleep(interval)
                    client = self._reopen_connection()
                    if client:
                        break
                if no_retry:
                    raise exc
            finally:
                now = utc_timestamp()
                elapsed = now - t
                logger.debug("RPC call to %s took %d ms", _api, elapsed * 1E+3)
                # XXX each deque stores pairs of (utc timestamp, timing in seconds)
                self._response_timings[_api].append((now, round(elapsed, 6)))

    def _reopen_connection(self):
        """Reopen connection to HBase.

        Method should return a client instance or None on failed attempt.
        """
        logger.info("Trying to reconnect")
        try:
            self._connection._refresh_thrift_client()
            self._connection.open()
            client = super(RecoveringClient, self._connection.client)
        except TTransportException:
            logger.exception("Got exception, while trying to reconnect. Continuing")
        else:
            logger.debug("New client is initialized")
            return client

    def get_stats(self):
        """Get HBase client stats.

        Current implementation returns only response timings per each client function
        since last get_stats() call.
        """
        stats = {}
        for api_entry, api_timings in self._response_timings.items():
            stats['happybase.' + api_entry] = list(api_timings)
        self._response_timings.clear()
        return stats
