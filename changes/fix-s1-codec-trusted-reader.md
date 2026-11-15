Changed

- Response translation through the new codecs spends less CPU per event: a canonical record held in one storage frame is read in one pass over its bytes instead of byte by byte, and its correlation fields are found in one walk instead of one lookup each.
