# Helpers for the store iterator test pipelines: read a store iterator and return its entries as text.

def read_has_next(it, describe):
  # The old style: hasNext()/next() without close()
  entries = []
  while it.hasNext():
    entries.append(describe(it.next()))
  return ",".join(entries)

def read(make, describe):
  # Reads one iterator with a for loop, and a second one with hasNext()/next() without close()
  for_loop = ",".join(describe(e) for e in make())
  has_next = read_has_next(make(), describe)
  return for_loop if for_loop == has_next else for_loop + "!hasNext=" + has_next
