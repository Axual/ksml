from typing import TYPE_CHECKING

if TYPE_CHECKING:
  # this import only happens during development, enables code completion/syntax highlighting
  from ksml_runtime import log

def check_sensor_is_blue(value, key):
  if value == None:
    log.warn("No value in message with key={}", key)
    return False
  if value["color"] != "blue":
    log.warn("Unknown color: {}", value["color"])
    return False
  return True
