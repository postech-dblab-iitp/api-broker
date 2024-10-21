[![English](
https://img.shields.io/badge/language-English-orange.svg)](README_EN.md)
[![Korean](
https://img.shields.io/badge/language-Korean-blue.svg)](README.md)

## TurboGraph JDBC driver 위한 API-Broker

## 소스로 부터 빌드하기
### 빌드 요구사항

- TurboGraph++ library를 사용해야 하므로 TubroGraph++를 빌드할 수 있는 docker container를 기반으로 구성해야 합니다.
- Docker 설치 및 빌드에 대해서는 TurboGraph++ 저장소(https://github.com/postech-dblab-iitp/turbograph-v3)를 참고하세요. (제공되는 docker container는 ubuntu OS기반입니다.)
```
git clone https://github.com/postech-dblab-iitp/turbograph-v3.git
cd turbograph-v3
git checkout remotes/origin/main
cd docker
docker build . -t turbograph-image
./run-docker-example.sh <database folder> <source data folder>
```
### 빌드 실행 방법

1. Turbograph++ 소스를 다운 받아 빌드 합니다.
```
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
2. API-Broker 빌드를 위해 아래와 같은 패키지 설치가 필요합니다.
 - systemtap-sdt-dev
 - zip
```
apt install systemtap-sdt-dev
apt install zip
```

3. Turbograph++ 소스 위치로 이동하여 API Broker 소스를 다운 받아 빌드합니다.
```
cd /turbograph-v3
git clone git@github.com:postech-dblab-iitp/api-broker.git
cd api-broker
./build.sh 
```

## 설치 및 사용
### 설치 방법
아래와 같이 2가지 방식을 지원합니다.

1. Shell 파일을 사용
```
sh API-BROKER-1.0.0.0022-0b9a9e0-Linux.x86_64.sh
```

2. tar.gz를 사용

아래 파일을 참고하여 설치 하세요.

https://github.com/hwany7seo/iitp-api-broker/blob/packaging_readme/contrib/readme/README_TAR_INSTALL

또는

tar.gz 안에 README_TAR_INSTALL를 참고하세요.

### 데이터베이스 설정
API Broker가 설치된 폴더에 'data/databases.txt'을 수정해야 합니다.
아래 예제와 같이 추가하면 JDBC를 통해 tpch_sf1 과 gdbdemo 데이터베이스에 접근 할 수 있습니다.
```
tpch_sf1       /data/tpch/sf1
gdbdemo        /data/gdbdemo
```
### 실행 방법
- 시작
```
brokerutil start
```
- 중지
```
brokerutil stop
```
- 재시작
```
brokerutil restart <broker-name> <appl_server_index>
```
- 상태확인
```
broker_monitor
```

### API Broker 설정
'conf/api_broker.conf' 파일을 통해 API Broker를 설정할 수 있습니다.
아래는 query_editor와 BROKER1을 사용한 예제입니다.

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

아래 설명은 주로 변경하는 파라미터 정보입니다.
```
SERVICE : API Broker를 on 또는 off 

BROKER_PORT : Broker에서 사용하는 포트 번호

MIN_NUM_APPL_SERVER = 연결 요청이 없더라도 대기하는 기본적인 최소 프로세스 개수입니다. 

MAX_NUM_APPL_SERVER = 동사에 연결 할 수 있는 최대 프로세스 값입니다.

APPL_SERVER_MAX_SIZE = CAS가 처리하는 프로세스 메모리 사용량의 최대 크기를 지정하는 파라미터입니다.
 값 뒤에 B, K, M, G로 단위를 붙일 수 있으며, 각각 Bytes, Kilobytes, Megabytes, Gigabytes를 의미한다. 단위 생략 시 M으로 지정됩니다.
TurboGraph++ 저장소에 크기에 따라 값을 조정해야합니다.
```

## 라이센스

- Apache license 2.0

## 도움 받기

http://jira.iitp.cubrid.org/secure/Dashboard.jspa

버그, 개선 사항, 질문이 있는 경우 위 jira에 내용을 남기면 지원을 받을 수 있습니다.
