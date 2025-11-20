def check_sensor_is_blue(value, key):
  if value == None:
    log.warn("No value in message with key={}", key)
    return False
  if value["color"] != "blue":
    log.warn("Unknown color: {}", value["color"])
    return False
  return True
