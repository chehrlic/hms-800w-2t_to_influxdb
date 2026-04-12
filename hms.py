#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import asyncio
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from google.protobuf.json_format import MessageToJson
from google.protobuf.message import Message
from hoymiles_wifi.dtu import DTU, NetworkState
from hoymiles_wifi.protobuf import (
    RealDataNew_pb2,
)
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from suntimes import SunTimes
import yaml
from yaml.loader import SafeLoader

DEFAULT_CONFIG = {
    'host': None,
    'interval': 5,
    'logging': {
        'level': 'ERROR',
    },
    'sunset': {
        'disabled': True,
        'latitude': None,
        'longitude': None,
        'altitude': None,
    },
    'influx': {
        'url': None,
        'org': None,
        'token': None,
        'bucket': None,
        'measurement': 'hoymiles',
    },
}


def ensure_utc(value):
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class SunsetHandler:
    def __init__(self, sunset_config):
        self.suntimes = None
        if sunset_config and sunset_config.get('disabled', True) == False:
            latitude = sunset_config.get('latitude')
            longitude = sunset_config.get('longitude')
            altitude = sunset_config.get('altitude')
            self.suntimes = SunTimes(longitude=longitude, latitude=latitude, altitude=altitude)
            self.nextSunset = ensure_utc(self.suntimes.setutc(datetime.now(timezone.utc)))
            logging.info(f'Todays sunset is at {self.nextSunset} UTC')
        else:
            logging.info('Sunset disabled.')

    async def checkWaitForSunrise(self):
        if not self.suntimes:
            return
        # if the sunset already happened for today
        now = datetime.now(timezone.utc)
        if self.nextSunset < now:
            # wait until the sun rises again. if it's already after midnight, this will be today
            nextSunrise = ensure_utc(self.suntimes.riseutc(now))
            if nextSunrise < now:
                tomorrow = now + timedelta(days=1)
                nextSunrise = ensure_utc(self.suntimes.riseutc(tomorrow))
            self.nextSunset = ensure_utc(self.suntimes.setutc(nextSunrise))
            time_to_sleep = int((nextSunrise - datetime.now(timezone.utc)).total_seconds())
            logging.info (f'Next sunrise is at {nextSunrise} UTC, next sunset is at {self.nextSunset} UTC, sleeping for {time_to_sleep} seconds.')
            if time_to_sleep > 0:
                await asyncio.sleep(time_to_sleep)
                logging.info (f'Woke up...')

def init_logging(hoymiles_config):
    log_config = hoymiles_config.get('logging')

    lvl = logging.ERROR
    if log_config:
        level = log_config.get('level', 'ERROR')
        if level == 'DEBUG':
            lvl = logging.DEBUG
        elif level == 'INFO':
            lvl = logging.INFO
        elif level == 'WARNING':
            lvl = logging.WARNING
        elif level == 'ERROR':
            lvl = logging.ERROR
        elif level == 'FATAL':
            lvl = logging.FATAL

    logging.basicConfig(
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S.%s', level=lvl, force=True)
    logging.info(f'start logging with level: {logging.getLevelName(logging.root.level)}')


def apply_env_overrides(config, prefix=()):
    for key, value in config.items():
        env_key = '_'.join([*prefix, key]).upper()
        if isinstance(value, dict):
            apply_env_overrides(value, (*prefix, key))
            continue
        if env_key not in os.environ:
            continue
        config[key] = yaml.load(os.environ[env_key], Loader=SafeLoader)


def merge_config(base, overrides):
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            merge_config(base[key], value)
            continue
        base[key] = value
    return base

# Inverter commands
async def async_get_real_data_new(
    dtu: DTU,
) -> RealDataNew_pb2.RealDataNewResDTO | None:
    '''Get real data from the inverter asynchronously.'''

    async with asyncio.timeout(10):
        return await dtu.async_get_real_data_new()
    raise TimeoutError


async def main() -> None:
    parser = argparse.ArgumentParser(description='Hoymiles DTU Monitoring')
    parser.add_argument(
        '--config', type=str, help='YAML config file, defaults to hoymiles.yml', default='hoymiles.yml'
    )
    args = parser.parse_args()

    try:
        hoymilescfg = deepcopy(DEFAULT_CONFIG)
        try:
            with open(args.config, 'r') as fh_yaml:
                cfg = yaml.load(fh_yaml, Loader=SafeLoader)
            merge_config(hoymilescfg, cfg or {})
        except FileNotFoundError:
            logging.info(f'Config file {args.config} not found, using environment variables and defaults.')
        apply_env_overrides(hoymilescfg)
        init_logging(hoymilescfg)

        sunset = SunsetHandler(hoymilescfg.get('sunset'))

        interval = hoymilescfg.get('interval', 5)
        hoymiles_host = hoymilescfg.get('host')
        logging.info(f'Using hoymiles (source) address: {hoymiles_host}, requesting data every {interval} seconds')
        dtu = DTU(hoymiles_host)

        influxcfg = hoymilescfg.get('influx', {})
        logging.info(f'Influx config: {influxcfg}')
        influxorg = influxcfg.get('org')
        influxbucket = influxcfg.get('bucket')
        influxmeasurement = influxcfg.get('measurement', 'hoymiles')
        influxclient = InfluxDBClient(influxcfg.get('url'), influxcfg.get('token'), bucket=influxbucket)
        infuxconn = influxclient.write_api(write_options=SYNCHRONOUS)
    except yaml.YAMLError as e:
        logging.error(f'Failed to load config file {args.config}: {e}')
        sys.exit(1)
    except Exception as e:
        logging.fatal (f'Exception during setup from config file {args.config}: {e}')
        sys.exit(1)

    while True:
        try:
            await sunset.checkWaitForSunrise()
            response = await async_get_real_data_new(dtu)
            if response and isinstance(response, Message):
                data = json.loads(MessageToJson(response))
                logging.debug(f'data from hoymiles: {data}')

                measurement = influxmeasurement + f',location={data["deviceSerialNumber"]}'

                data_stack = []
                if 'time' in data and isinstance(data['timestamp'], datetime):
                    time_rx = data['time']
                else:
                    time_rx = datetime.now()

                # InfluxDB uses UTC
                utctime = datetime.fromtimestamp(time_rx.timestamp(), tz=timezone.utc)

                # InfluxDB requires nanoseconds
                ctime = int(utctime.timestamp() * 1e9)

                atLeastOneAdded = False
                # AC Data
                phase_id = 0
                for phase in data['sgsData']:
                    try:
                        data_stack += [f'{measurement},phase={phase_id},type=voltage value={phase["voltage"]/10} {ctime}',
                                       f'{measurement},phase={phase_id},type=current value={phase["current"]/100} {ctime}',
                                       f'{measurement},phase={phase_id},type=power value={phase["activePower"]/10} {ctime}',
                                       f'{measurement},phase={phase_id},type=frequency value={phase["frequency"]/100:.3f} {ctime}',
                                       f'{measurement},phase={phase_id},type=temperature value={phase["temperature"]/10} {ctime}',
                                      ]
                        phase_id = phase_id + 1
                        atLeastOneAdded = True
                    except:
                        pass

                # DC Data
                for string in data['pvData']:
                    try:
                        port_number = int(string['portNumber']) - 1
                        data_stack += [f'{measurement},port={port_number},type=voltage value={string["voltage"]/10:.3f} {ctime}',
                                       f'{measurement},port={port_number},type=current value={string["current"]/100:3f} {ctime}',
                                       f'{measurement},port={port_number},type=power value={string["power"]/10:.2f} {ctime}',
                                       f'{measurement},port={port_number},type=YieldDay value={string["energyDaily"]:.2f} {ctime}',
                                       f'{measurement},port={port_number},type=YieldTotal value={string["energyTotal"]:.4f} {ctime}'
                                      ]
                        atLeastOneAdded = True
                    except:
                        pass
                if atLeastOneAdded:
                    logging.debug(f'data to influx: {data_stack}')
                    infuxconn.write(influxbucket, influxorg, data_stack)
            elif response:
                logging.warning (f'Unhandled message {response}')
        except json.JSONDecodeError as e:
            logging.error (f'Json decode exception: {e}')
        except ValueError as e:
            logging.error (f'Json exception: {e}')
        except TimeoutError as e:
            logging.error (f'Timeout while trying to retrieve data from DTU.')
        except Exception as e:
            logging.error (f'Runtime exception: {e}')
        if dtu.get_state() == NetworkState.Online:
            await asyncio.sleep(interval)

def run_main() -> None:
    '''Run the main function for the hoymiles_wifi package.'''

    asyncio.run(main())


if __name__ == '__main__':
    run_main()
