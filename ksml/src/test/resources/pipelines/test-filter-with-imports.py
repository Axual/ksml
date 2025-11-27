from typing import TYPE_CHECKING
import random  # test standard module import

if TYPE_CHECKING:
  # this import only happens during development, enables code completion/syntax highlighting
  from ksml_runtime_stub import log

def values_is_blue(somekey, someval):
  log.info("calling random returned: {}", random.randrange(10))
  if someval == None:
    log.warn("No value in message with key={}", somekey)
    return False
  if someval["color"] != "blue":
    log.warn("Unknown color: {}", someval["color"])
    return False
  return True
