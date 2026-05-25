CREATE TABLE IF NOT EXISTS collector.ip (
   bantime DateTime64(3),
   ipv4 IPv4,
   ipv6 IPv6
) ENGINE = MergeTree()
ORDER BY (bantime);