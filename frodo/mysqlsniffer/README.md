## debug
java -jar  mysqlsniffer-1.0-SNAPSHOT.jar --port=3306 --username=root --password=Adam_123 --dst-port=3306 --dst-ip=127.0.0.1 --dst-username=root --dst-password=Adam_123 --network-device=eth0  --concurrency=1  --warm-time=1000 --log-level=info -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005

## capture
java  -jar mysqlsniffer.jar --capture-method=net_capture   --replay-to=file --port=3306 --username=root --password=abc123456 --network-device=eth0  --concurrency=32  --time=1000 --log-level=info 

## general_log
java  -jar mysqlsniffer.jar --capture-method=general_log  --replay-to=file --port=3306 --username=root --password=Abc123456@  --concurrency=32 --time=60


