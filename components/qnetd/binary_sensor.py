import esphome.codegen as cg
import esphome.config_validation as cv
from esphome.components import binary_sensor
from esphome.const import ENTITY_CATEGORY_DIAGNOSTIC

from . import CONF_QNETD_ID, QnetdComponent

CONF_VOTE_GRANTED = "vote_granted"

CONFIG_SCHEMA = cv.Schema(
    {
        cv.GenerateID(CONF_QNETD_ID): cv.use_id(QnetdComponent),
        cv.Optional(CONF_VOTE_GRANTED): binary_sensor.binary_sensor_schema(
            entity_category=ENTITY_CATEGORY_DIAGNOSTIC,
        ),
    }
)


async def to_code(config):
    parent = await cg.get_variable(config[CONF_QNETD_ID])
    if CONF_VOTE_GRANTED in config:
        sens = await binary_sensor.new_binary_sensor(config[CONF_VOTE_GRANTED])
        cg.add(parent.set_vote_granted_sensor(sens))
