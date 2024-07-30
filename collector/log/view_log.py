import gzip

with gzip.open('/home/lamu/hftbacktest/collector/log/btc-usdt_20240726.gz', 'r') as f:
    for i in range(30):
        line = f.readline()
        print(line)
