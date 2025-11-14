def is_blue(val):
    """ Filter values in the stream with attribute 'color' having value 'blue'"""
    if val == None:
        # log.warn("No value in message with key={}", key)  <==== NEED TO FIX THIS, NOT PRESENT IN IMPORT MODULE!
        return False
    if val["color"] != "blue":
        # log.warn("Unknown color: {}", val["color"])
        return False
    return True
