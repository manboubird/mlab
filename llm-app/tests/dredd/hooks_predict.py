import dredd_hooks as hooks
import logging
from pprint import pformat


LOGFILE = 'logs/dred-hook-python.log'
logging.basicConfig(filename=LOGFILE,
                    filemode='a',
                    format='%(asctime)s.%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__.split('.')[0])


@hooks.before_all
def my_before_all_hook(transactions):
    logger.debug('before all')


@hooks.before_each
def my_before_each_hook(transaction):
    logger.debug('before each')


@hooks.before
def my_before_hook(transaction):
    logger.debug('before')


@hooks.before_each_validation
def my_before_each_validation_hook(transaction):
    logger.debug('before each validation')


@hooks.before_validation
def my_before_validation_hook(transaction):
    logger.debug('before validations')


@hooks.after
def my_after_hook(transaction):
    logger.debug('after')


@hooks.after_each
def my_after_each(transaction):
    logger.debug('after_each')
    # logger.debug('transaction: {}'.format(pformat(transaction)))
    logger.debug('transaction: response header server-timing: {}'.format(pformat(transaction['results']['fields']['headers']['values']['actual']['server-timing'])))
    

@hooks.after_all
def my_after_all_hook(transactions):
    logger.debug('after_all')
    # logger.debug('transactions: {}'.format(pformat(transactions)))
