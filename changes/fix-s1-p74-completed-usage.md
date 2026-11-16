Fixed

- A completed Antigravity answer to a Claude client records only the usage counts the upstream sent, as a failed attempt already does. A count the upstream left out stays absent instead of being recorded as 0 or derived from the total, and an explicit 0 stays 0.
- The total token count an Antigravity upstream reports is recorded as sent, including when the upstream reports only the total. Before, a total-only report recorded no total.
