"""qnetd: a corosync-qnetd quorum arbiter for two-node clusters.

The ESP is the qnetd *server*; the cluster nodes run stock corosync-qdevice
pointed at it. ffsplit only, plaintext (advertises TLS-unsupported).
"""

import esphome.codegen as cg
import esphome.config_validation as cv
from esphome.const import CONF_ID, CONF_PORT

CODEOWNERS = ["@fhedberg"]
DEPENDENCIES = ["network"]
# the component owns a TCP listener; socket is not implied by `network`
AUTO_LOAD = ["socket"]

CONF_QNETD_ID = "qnetd_id"

qnetd_ns = cg.esphome_ns.namespace("qnetd")
QnetdComponent = qnetd_ns.class_("QnetdComponent", cg.Component)

CONFIG_SCHEMA = cv.Schema(
    {
        cv.GenerateID(): cv.declare_id(QnetdComponent),
        cv.Optional(CONF_PORT, default=5403): cv.port,
    }
).extend(cv.COMPONENT_SCHEMA)


async def to_code(config):
    var = cg.new_Pvariable(config[CONF_ID])
    await cg.register_component(var, config)
    cg.add(var.set_port(config[CONF_PORT]))
