from TwitterAPI import TwitterAPI
from TwitterAPI import BearerAuth as OAuth2
from datetime import datetime
from requests.exceptions import ConnectionError, ReadTimeout, SSLError
from urllib3.exceptions import ReadTimeoutError, ProtocolError
from requests_oauthlib import OAuth1
from TwitterAPI.TwitterError import *
import json
import requests
import socket
import ssl
import time
import os

class StreamingIterableOverride(object):

    """Iterate statuses or other objects in a Streaming API response.
    :param response: The request.Response from a Twitter Streaming API request
    """

    def __init__(self, response):
        self.stream = response.raw

    def _iter_stream(self):
        """Stream parser.
        :returns: Next item in the stream (may or may not be 'delimited').
        :raises: TwitterConnectionError, StopIteration
        """
        while True:
            item = None
            buf = bytearray()
            stall_timer = None
            try:
                while True:
                    # read bytes until item boundary reached
                    buf += self.stream.read(1)
                    if not buf:
                        # check for stall (i.e. no data for 90 seconds)
                        if not stall_timer:
                            stall_timer = time.time()
                        elif time.time() - stall_timer > TwitterAPI.STREAMING_TIMEOUT:
                            raise TwitterConnectionError('Twitter stream stalled')
                    elif stall_timer:
                        stall_timer = None
                    if buf[-2:] == b'\r\n':
                        item = buf[0:-2]
                        if item.isdigit():
                            # use byte size to read next item
                            nbytes = int(item)
                            item = None
                            item = self.stream.read(nbytes)
                        else:
                            item = b'{"stop": true}'
                        break
                yield item
            except (ConnectionError, ProtocolError, ReadTimeout, ReadTimeoutError,
                    SSLError, ssl.SSLError, socket.error) as e:
                raise TwitterConnectionError(e)
            except AttributeError:
                # inform iterator to exit when client closes connection
                raise StopIteration

    def __iter__(self):
        """Iterator.
        :returns: Tweet status as a JSON object.
        :raises: TwitterConnectionError
        """
        for item in self._iter_stream():
            if item:
                try:
                    yield json.loads(item.decode('utf8'))
                except ValueError as e:
                    # invalid JSON string (possibly an unformatted error message)
                    raise TwitterConnectionError(e)

def get_iterator_override(self):
    if self.response.status_code != 200:
        raise TwitterRequestError(self.response.status_code)

    return iter(StreamingIterableOverride(self.response))