import esphome.codegen as cg
import esphome.config_validation as cv
from esphome.components import text_sensor
from esphome.const import ENTITY_CATEGORY_DIAGNOSTIC

from . import CONF_QNETD_ID, QnetdComponent

CONF_STATUS = "status"

CONFIG_SCHEMA = cv.Schema(
    {
        cv.GenerateID(CONF_QNETD_ID): cv.use_id(QnetdComponent),
        cv.Optional(CONF_STATUS): text_sensor.text_sensor_schema(
            entity_category=ENTITY_CATEGORY_DIAGNOSTIC,
        ),
    }
)


async def to_code(config):
    parent = await cg.get_variable(config[CONF_QNETD_ID])
    if CONF_STATUS in config:
        sens = await text_sensor.new_text_sensor(config[CONF_STATUS])
        cg.add(parent.set_status_sensor(sens))
