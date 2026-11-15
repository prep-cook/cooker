Internal

- Client response egress commits at the first byte the client's codec emits: the client gets no status or header before it, an attempt that fails first leaves its request for another attempt, and a committed response never fails over or waits for a slow client.
