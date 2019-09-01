#!/home/dsmr/dsmr-venv/bin/python3

import sys
print("Importing...", file=sys.stderr, flush=True)

from aioinflux import InfluxDBClient, iterpoints
import asyncio
from base64 import b16decode
from datetime import datetime
from dsmr_parser.clients import create_dsmr_reader
from dsmr_parser import obis_references as obis_ref
import re
#import warnings
#warnings.filterwarnings('ignore', message=".*dataframe.*")

# Monkey patch to get Decimal results back from aioinflux instead of floats with loss of precision
from decimal import Decimal
import json
old_json_loads = json.loads

def loads(*args, **kwargs):
    kwargs.setdefault('parse_float', Decimal)
    return old_json_loads(*args, **kwargs)
json.loads = loads
del loads

sensor_names = {
        obis_ref.ELECTRICITY_USED_TARIFF_1             : ('power_consumption_low'     , lambda v: {'value': v}),
        obis_ref.ELECTRICITY_USED_TARIFF_2             : ('power_consumption_normal'  , lambda v: {'value': v}),
        obis_ref.ELECTRICITY_DELIVERED_TARIFF_1        : ('power_production_low'      , lambda v: {'value': v}),
        obis_ref.ELECTRICITY_DELIVERED_TARIFF_2        : ('power_production_normal'   , lambda v: {'value': v}),
        obis_ref.ELECTRICITY_ACTIVE_TARIFF             : ('power_tariff'              , lambda v: {'state': {'0001': 'low', '0002': 'normal'}[v]}),
        obis_ref.CURRENT_ELECTRICITY_USAGE             : ('power_consumption'         , lambda v: {'value': v}),
        obis_ref.CURRENT_ELECTRICITY_DELIVERY          : ('power_production'          , lambda v: {'value': v}),
        obis_ref.INSTANTANEOUS_VOLTAGE_L1              : ('voltage_phase_l1'          , lambda v: {'value': v}),
        obis_ref.INSTANTANEOUS_VOLTAGE_L2              : ('voltage_phase_l2'          , lambda v: {'value': v}),
        obis_ref.INSTANTANEOUS_VOLTAGE_L3              : ('voltage_phase_l3'          , lambda v: {'value': v}),
        #obis_ref.VOLTAGE_SAG_L1_COUNT                  : ('voltage_sags_phase_l1'     , lambda v: {'value': v}),
        #obis_ref.VOLTAGE_SAG_L2_COUNT                  : ('voltage_sags_phase_l2'     , lambda v: {'value': v}),
        #obis_ref.VOLTAGE_SAG_L3_COUNT                  : ('voltage_sags_phase_l3'     , lambda v: {'value': v}),
        #obis_ref.VOLTAGE_SWELL_L1_COUNT                : ('voltage_swells_phase_l1'   , lambda v: {'value': v}),
        #obis_ref.VOLTAGE_SWELL_L2_COUNT                : ('voltage_swells_phase_l2'   , lambda v: {'value': v}),
        #obis_ref.VOLTAGE_SWELL_L3_COUNT                : ('voltage_swells_phase_l3'   , lambda v: {'value': v}),
        #obis_ref.LONG_POWER_FAILURE_COUNT              : ('long_power_failure_count'  , lambda v: {'value': v}),
        obis_ref.INSTANTANEOUS_ACTIVE_POWER_L1_POSITIVE: ('power_consumption_phase_l1', lambda v: {'value': v}),
        obis_ref.INSTANTANEOUS_ACTIVE_POWER_L2_POSITIVE: ('power_consumption_phase_l2', lambda v: {'value': v}),
        obis_ref.INSTANTANEOUS_ACTIVE_POWER_L3_POSITIVE: ('power_consumption_phase_l3', lambda v: {'value': v}),
        obis_ref.INSTANTANEOUS_ACTIVE_POWER_L1_NEGATIVE: ('power_production_phase_l1' , lambda v: {'value': v}),
        obis_ref.INSTANTANEOUS_ACTIVE_POWER_L2_NEGATIVE: ('power_production_phase_l2' , lambda v: {'value': v}),
        obis_ref.INSTANTANEOUS_ACTIVE_POWER_L3_NEGATIVE: ('power_production_phase_l3' , lambda v: {'value': v}),
        obis_ref.HOURLY_GAS_METER_READING              : ('gas_consumption'           , lambda v: {'value': v}),
    }

last_only_re    = re.compile(r'power_(?:consumption|production)$|power_(?:consumption|production)_(?:low|normal)|power_tariff|gas_consumption$|voltage_.*$|.*_(?:sags|swells)_.*$|.*_count$')
nonzero_only_re = re.compile(r'power_(?:consumption|production)$')
once_per_day_re = re.compile(r'power_(?:consumption|production)_(?:low|normal)|power_tariff|gas_consumption$|voltage_.*$')
once_per_day_re = re.compile(r'^(?=.)$')

async def transmit_telegram(ifx, telegram):
    try:
        now = telegram.pop(obis_ref.P1_MESSAGE_TIMESTAMP).value
    except KeyError:
        now = datetime.utcnow()
    try:
        serial = b16decode(telegram.pop(obis_ref.EQUIPMENT_IDENTIFIER).value).decode('ASCII')
    except KeyError:
        serial = None
    try:
        serial_gas = b16decode(telegram.pop(obis_ref.EQUIPMENT_IDENTIFIER_GAS).value).decode('ASCII')
    except KeyError:
        serial_gas = None
    points = []
    for obiref, obj in telegram.items():
        tags = {
                'domain':   'sensor',
                'type':     'dsmr',
            }

        try:
            tags['entity_id'], make_fields = sensor_names[obiref]
        except KeyError:
            for name in dir(obis_ref):
                if getattr(obis_ref, name) == obiref:
                    #debug:print(name, obj.value, obj.unit, file=sys.stderr, flush=True)
                    break
            continue

        if serial:
            tags['instance'] = serial
        if 'gas_consumption' in tags['entity_id'] and serial_gas:
            tags['instance'] = serial_gas

        points.append({
                'time':        getattr(obj, 'datetime', now),
                'measurement': obj.unit or f"{tags['domain']}.{tags['entity_id']}",
                'tags':        tags,
                'fields':      make_fields(obj.value),
            })

    # Skip unchanged states
    queries = []
    for idx, point in enumerate(points):
        (field, value), = point['fields'].items()
        if last_only_re.match(point['tags']['entity_id']):
            instance_expr = (f"AND instance='{point['tags']['instance']}'" if 'instance' in point['tags'] else '')
            once_per_day_expr = ('AND time > now()-1d' if once_per_day_re.match(point['tags']['entity_id']) else 'AND time > now()-7d')
            queries.append((idx, f"SELECT last(\"{field}\") FROM \"{point['measurement']}\" WHERE \"entity_id\"='{point['tags']['entity_id']}' {instance_expr} {once_per_day_expr} ORDER BY time"))

    statements = sorted((statement
            for statement in (await ifx.query(';'.join(query for idx, query in queries)))['results']
            if 'series' in statement),
            key=lambda s: s['statement_id'],
            reverse=True)
    for statement in statements:
        idx, _ = queries[statement['statement_id']]
        point = points[idx]
        (field, value), = point['fields'].items()
        try:
            serie, = statement['series']
            rows = serie['values']
            rows[0]
            row, = rows
            prev_time, prev_value = row
        except (KeyError, IndexError):
            pass
        else:
            #debug:print(f"@{prev_time} {idx:>2d}: {point['tags']['entity_id']:<26s}: {prev_value!r}=={value!r}=>{value == prev_value}", file=sys.stderr, flush=True)
            if value == prev_value and (
                    not nonzero_only_re.match(point['tags']['entity_id'])
                    or prev_value == 0):
                points[idx] = None
                continue
    points = [point for point in points if point]

    #debug:import pprint
    #debug:pprint.pprint(points, stream=sys.stderr)
    #debug:print(file=sys.stderr, flush=True)
    if points:
        await ifx.write(points)

async def main():
    async with InfluxDBClient(db='dsmr', output='json') as ifx:
        transport, protocol = await create_dsmr_reader(port, dsmr_version, lambda telegram: loop.create_task(transmit_telegram(ifx, telegram)), loop=loop)
        print(f"Opened {port} for DSMR {dsmr_version} reading", file=sys.stderr, flush=True)
        try:
            await protocol.wait_closed()
        except KeyboardInterrupt:
            transport.close()
            await asyncio.sleep(0)

print("Starting asyncio...", file=sys.stderr, flush=True)
port = '/dev/ttyUSB0'
dsmr_version = '2.2'
loop = asyncio.get_event_loop()
loop.run_until_complete(main())
