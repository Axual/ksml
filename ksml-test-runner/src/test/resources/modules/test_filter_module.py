def is_blue(val):
    """ Filter values in the stream with attribute 'color' having value 'blue'"""
    if val == None:
        return False
    if val["color"] != "blue":
        return False
    return True

def is_red(val):
    """ Filter values in the stream with attribute 'color' having value 'red'"""
    if val == None:
        return False
    if val["color"] != "red":
        return False
    return True
