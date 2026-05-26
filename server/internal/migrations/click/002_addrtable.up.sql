CREATE TABLE IF NOT EXISTS collector.ip (
   bantime DateTime64(3),
   ip String
) ENGINE = MergeTree()
ORDER BY (bantime,ip);