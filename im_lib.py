import psycopg2

import logging

import sqlalchemy

from sqlalchemy import create_engine

from psycopg2.extensions import AsIs

import numpy as np

from datetime import datetime, timedelta, date, time

from numpy import ndarray, arange

import xlsxwriter

from psycopg2.extensions import register_adapter, AsIs

psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)


import pandas as pd

import copy

from pandas import DataFrame

import json

import logging

logging.basicConfig(filename='test.log',level=logging.INFO)

def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)

def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)

register_adapter(np.float64, addapt_numpy_float64)

register_adapter(np.int64, addapt_numpy_int64)


import csv




