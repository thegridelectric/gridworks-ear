"""Paths and loggers for the ear (XDG Base Directory convention)."""

import logging

import xdg

EAR_LOG_FORMAT_STR = "%(asctime)s - %(message)s"

EAR_LOG_NAME = "ear"
EAR_STATE_LOG_NAME = EAR_LOG_NAME + ".state"
EAR_MESSAGE_LOG_NAME = EAR_LOG_NAME + ".message"

EAR_LOGGER = logging.getLogger(EAR_LOG_NAME)
EAR_STATE_LOGGER = logging.getLogger(EAR_STATE_LOG_NAME)
EAR_MESSAGE_LOGGER = logging.getLogger(EAR_MESSAGE_LOG_NAME)

_EAR_SUBDIR = "gridworks/ear"
OUTPUT_DIRECTORY = xdg.xdg_data_home() / _EAR_SUBDIR / "output"
STATE_DIRECTORY = xdg.xdg_state_home() / _EAR_SUBDIR
LOG_DIRECTORY = STATE_DIRECTORY / "log"
EAR_STATE_LOG_PATH = LOG_DIRECTORY / "state.txt"
EAR_MESSAGE_LOG_PATH = LOG_DIRECTORY / "message.txt"
LOG_MESSAGE_BYTES = 1 * 1024 * 1024
LOG_BACKUPS = 4
