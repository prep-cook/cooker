Changed

- Request translation through the new codecs binds each published request field from the position it wrote, instead of decoding the field's record again, which cuts per-request CPU time.
