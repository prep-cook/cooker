Changed

- Response translation through the new codecs spends less CPU per event: a canonical record held in one storage frame is read in one pass over its bytes instead of byte by byte, and an egress checks the stream identity once on the first record instead of on every record.
