[![English](
https://img.shields.io/badge/language-English-orange.svg)](README_EN.md)
[![Korean](
https://img.shields.io/badge/language-Korean-blue.svg)](README.md)

## API-Broker for TurboGraph JDBC driver 

## Build from Source
### Build Requirements
- Since we need to use the TurboGraph++ library, we need to configure it based on a docker container that can build TubroGraph++.
- For Docker installation and build, please refer to the TurboGraph++ repository (https://github.com/postech-dblab-iitp/turbograph-v3). (The provided docker container is based on Ubuntu OS.)
```
git clone https://github.com/postech-dblab-iitp/turbograph-v3.git
cd turbograph-v3
git checkout remotes/origin/main
cd docker
docker build . -t turbograph-image
./run-docker-example.sh <database folder> <source data folder>
```

### How to Build API Broker

1. Download turbograph++ Source And Build.
```
cd /
git clone https://github.com/postech-dblab-iitp/turbograph-v3.git
cd /turbograph-v3
git checkout main
cd tbgpp-common/third_party/velox
./scripts/setup-ubuntu.sh
cd /turbograph-v3
mkdir build
cd build/
cmake -GNinja -DCMAKE_BUILD_TYPE=Debug ..
ninja
```
2. The following package installation for api broker is required.
- systemtap-sdt-dev
 - zip
```
apt install systemtap-sdt-dev
apt install zip
```

3. Go to the Turbograph++ source location and download the API Broker source and build it.
```
cd /turbograph-v3
git clone git@github.com:postech-dblab-iitp/api-broker.git
cd api-broker
./build.sh 
```

## Install And Use
### How to Install
Support the following methods:

1. Use Shell
```
sh API-BROKER-1.0.0.0022-0b9a9e0-Linux.x86_64.sh
```

2. Use tar.gz
Please refer to the files below when installing.

https://github.com/hwany7seo/iitp-api-broker/blob/packaging_readme/contrib/readme/README_TAR_INSTALL
Or
README_TAR_INSTALL in tar.gz

### How to Setup Database
Edit the 'data/databases.txt' files in the folder where API Broker is installed. 
If you add the example below, you can access the databases named tpch_sf1 and gdbdemo through jdbc.
```
tpch_sf1       /data/tpch/sf1
gdbdemo        /data/gdbdemo
```

### How to Execute
- Start
```
brokerutil start
```
- Stop
```
brokerutil stop
```
- Restart
```
brokerutil restart <broker-name> <appl_server_index>
```
- Status
```
broker_monitor
```

### Setup to API Broker
Settings for 'API-Broker' can be made through the 'conf/api_broker.conf' file.
The example below uses two API Brokers for query_editor and BROKER1.
```
[api_broker]
MASTER_SHM_ID           =30001
ADMIN_LOG_FILE          =log/api_broker/api_broker.log

[%query_editor]
SERVICE                 =ON
SSL         =OFF
BROKER_PORT             =30000
MIN_NUM_APPL_SERVER     =1
MAX_NUM_APPL_SERVER     =1
APPL_SERVER_SHM_ID      =30000
LOG_DIR                 =log/api_broker/sql_log
ERROR_LOG_DIR           =log/api_broker/error_log
SQL_LOG                 =ON
TIME_TO_KILL            =120
SESSION_TIMEOUT         =300
KEEP_CONNECTION         =AUTO
APPL_SERVER_MAX_SIZE    =40G
APPL_SERVER_MAX_SIZE_HARD_LIMIT =50G

[%BROKER1]
SERVICE                 =ON
SSL         =OFF
BROKER_PORT             =33000
MIN_NUM_APPL_SERVER     =5
MAX_NUM_APPL_SERVER     =40
APPL_SERVER_SHM_ID      =33000
LOG_DIR                 =log/api_broker/sql_log
ERROR_LOG_DIR           =log/api_broker/error_log
SQL_LOG                 =ON
TIME_TO_KILL            =120
SESSION_TIMEOUT         =300
KEEP_CONNECTION         =AUTO
```

This is a description of the main key parameters.
```
SERVICE : Turn API Broker on or off.

BROKER_PORT : Port number to be used by the broker.

MIN_NUM_APPL_SERVER = Even if there is no connection request, 
the number of waiting processes is basically the minimum.

MAX_NUM_APPL_SERVER = The maximum process value that can be connected simultaneously.

APPL_SERVER_MAX_SIZE = This parameter specifies the maximum size of process memory usage processed by API Broker. 
Units such as B, K, M, and G can be added to the value, 
meaning Bytes, Kilobytes, Megabytes, and Gigabytes, respectively. 
If the unit is omitted, it is designated as M.
Adjust the value according to the storage size in TurboGraph++.
```

## License

- Apache license 2.0

## Get Help

http://jira.iitp.cubrid.org/secure/Dashboard.jspa

If you have any bugs, improvements, or questions, you can receive support by leaving a comment in the jira above.