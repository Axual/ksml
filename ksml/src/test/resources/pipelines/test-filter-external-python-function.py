from typing import TYPE_CHECKING

if TYPE_CHECKING:
  # this import only happens during development, enables code completion/syntax highlighting
  from ksml_runtime import log

def values_is_blue(somekey, someval):
  if someval == None:
    log.warn("No value in message with key={}", somekey)
    return False
  if someval["color"] != "blue":
    log.warn("Unknown color: {}", someval["color"])
    return False
  return True
