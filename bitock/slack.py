from __future__ import annotations

# noinspection SpellCheckingInspection
__author__ = 'wookjae.jo'

import logging
from dataclasses import dataclass

from slack_sdk import WebClient

logger = logging.getLogger('notification')
logger.setLevel(logging.INFO)

STR_TS = 'ts'


class Sendable:
    pass


@dataclass
class Message(Sendable):
    text: str


@dataclass
class File(Sendable):
    comment: str
    path: str


class SlackApp:
    def __init__(self, token, channel):
        self.token = token
        self.client = WebClient(token=token)
        self.channel = channel

    def send(self, sendable: Sendable, ts=None):
        if isinstance(sendable, Message):
            response = self.client.chat_postMessage(
                channel=self.channel,
                text=sendable.text,
                thread_ts=ts)
        elif isinstance(sendable, File):
            response = self.client.files_upload(
                channels=self.channel,
                initial_comment=sendable.comment,
                file=sendable.path,
                thread_ts=ts,
            )
        else:
            raise RuntimeError(f'Not supported sendable type: {type(sendable)}')

        assert response.status_code == 200, f'Failed to send a message to {self.channel}\n{response}'
        return response.data.get('ts')


class _Warren(SlackApp):

    def __init__(self):
        super().__init__(token='xoxb-1605364850897-1622524239648-QxvKygr0ynO4cZQXhUkGUN01',
                         channel='#control-tower')


warren = _Warren()
