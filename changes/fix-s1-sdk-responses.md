Fixed

- Responses clients no longer receive cooker's internal `x_ck_opaque` member or events, which revealed the upstream protocol. An unknown event from a Responses upstream reaches a streaming Responses client as that event, numbered in sequence; unknown data from other protocols is dropped.
