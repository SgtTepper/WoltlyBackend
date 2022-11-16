import datetime
import logging
import requests

import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    requests.get('https://woltly.azurewebsites.net/api/dishdash?term=warmup&city=tel-aviv&searchdesc=false')

    logging.info('Warmup function ran at %s', utc_timestamp)
