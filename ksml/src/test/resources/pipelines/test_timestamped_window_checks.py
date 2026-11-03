# Checks for test-timestamped-window-store-methods-python.yaml: calls every method of a timestamped window store
# and returns the results as one line of text.

def key_of(e):
  return e["key"]["key"] if type(e["key"]) is dict else str(e["key"])

def plain(entries):
  return ",".join(key_of(e) + ":" + str(e["value"]["n"]) for e in entries)

def stamped(entries):
  return ",".join(key_of(e) + ":" + str(e["value"]["n"]) + "@" + str(e["timestamp"]) for e in entries)

def check(name, store):
  line = name
  line += " fetch=" + str(store.fetch("a", 0)["n"])
  line += " fetchWithTimestamp=" + str(store.fetchWithTimestamp("a", 0)["timestamp"])
  line += " missing=" + str(store.fetchWithTimestamp("x", 0))
  line += " fetchRange=" + plain(store.fetch("a", 0, 60000))
  line += " backwardFetchRange=" + plain(store.backwardFetch("a", 0, 60000))
  line += " fetchKeys=" + plain(store.fetch("a", "b", 0, 60000))
  line += " backwardFetchKeys=" + plain(store.backwardFetch("a", "b", 0, 60000))
  line += " all=" + plain(store.all())
  line += " backwardAll=" + plain(store.backwardAll())
  line += " fetchAll=" + plain(store.fetchAll(0, 60000))
  line += " backwardFetchAll=" + plain(store.backwardFetchAll(0, 60000))
  line += " fetchRangeWithTimestamp=" + stamped(store.fetchWithTimestamp("a", 0, 60000))
  line += " backwardFetchRangeWithTimestamp=" + stamped(store.backwardFetchWithTimestamp("a", 0, 60000))
  line += " fetchKeysWithTimestamp=" + stamped(store.fetchWithTimestamp("a", "b", 0, 60000))
  line += " backwardFetchKeysWithTimestamp=" + stamped(store.backwardFetchWithTimestamp("a", "b", 0, 60000))
  line += " allWithTimestamp=" + stamped(store.allWithTimestamp())
  line += " backwardAllWithTimestamp=" + stamped(store.backwardAllWithTimestamp())
  line += " fetchAllWithTimestamp=" + stamped(store.fetchAllWithTimestamp(0, 60000))
  line += " backwardFetchAllWithTimestamp=" + stamped(store.backwardFetchAllWithTimestamp(0, 60000))
  # Without a timestamp, put uses the timestamp of the record (3000)
  store.put("c", {"n": 9}, 60000)
  store.put("d", {"n": 7}, 60000, 777)
  line += " put=" + stamped(store.fetchWithTimestamp("c", "d", 60000, 60000))
  return line
