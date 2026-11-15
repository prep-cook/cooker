Changed

- Large requests use less memory: thinking settings, JSON field edits and `default` payload rules no longer copy the whole request body. A request whose thinking settings are already right allocates nothing in proportion to its size, and one that needs an edit allocates at most one more copy of the body.
