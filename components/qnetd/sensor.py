import esphome.codegen as cg
import esphome.config_validation as cv
from esphome.components import sensor
from esphome.const import (
    ENTITY_CATEGORY_DIAGNOSTIC,
    STATE_CLASS_MEASUREMENT,
    STATE_CLASS_TOTAL_INCREASING,
)

from . import CONF_QNETD_ID, QnetdComponent

CONF_CONNECTED_CLIENTS = "connected_clients"
CONF_DECISIONS = "decisions"

CONFIG_SCHEMA = cv.Schema(
    {
        cv.GenerateID(CONF_QNETD_ID): cv.use_id(QnetdComponent),
        cv.Optional(CONF_CONNECTED_CLIENTS): sensor.sensor_schema(
            accuracy_decimals=0,
            state_class=STATE_CLASS_MEASUREMENT,
            entity_category=ENTITY_CATEGORY_DIAGNOSTIC,
        ),
        cv.Optional(CONF_DECISIONS): sensor.sensor_schema(
            accuracy_decimals=0,
            state_class=STATE_CLASS_TOTAL_INCREASING,
            entity_category=ENTITY_CATEGORY_DIAGNOSTIC,
        ),
    }
)


async def to_code(config):
    parent = await cg.get_variable(config[CONF_QNETD_ID])
    if CONF_CONNECTED_CLIENTS in config:
        sens = await sensor.new_sensor(config[CONF_CONNECTED_CLIENTS])
        cg.add(parent.set_connected_sensor(sens))
    if CONF_DECISIONS in config:
        sens = await sensor.new_sensor(config[CONF_DECISIONS])
        cg.add(parent.set_decisions_sensor(sens))
