# Apache_Spark_Udemy
# Apache Spark Introduction: 아파치 스파크 소개

## 아파치 스파크 소개

***Multi Language Engine** for executing **data engineering, data science, and manchine learning** on **single-node** macheines or **clusters***

- 기존 map reduce에 비해 빠름 (in-memory, disk i/o 줄임, 10~100배 빠름)
- python / java / scala
- RDD (resilient distributed dataset)!
    - 1개의 노드가 장애가 나도 사용 가능 (distributed)
    - 가장 중요한 컨셉

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/e4510d8b-0409-4912-8b52-3b19dc28f331/Untitled.png)

- cluster manager가 task 분배
- executor
    - worker node 내에 생성
    - 1개의 cpu당 1개가 보통 실행되며 task를 수행함

### component

- 기본은 spark core
    - spark sql, dataframe
    - spark streaming
    - mllib (machine learning)
    - graphx

### python을 사용하는 이유?

- code의 간결성, dependency 관리 용이, compile 필요 X
- 범용성

⇒ industry level에서는 scala를 사용함

- native language기 때문에 훨씬 빠르고 new feature가 빠르게 적용됨

### RDD (resilient distributed dataset)

- Lazy Evaluation : action이 있기 전까지 계산을 시작하지 않음
- in-memory computation
- fault tolerant : 여러개의 노드에 데이터가 copy되어 저장 (?)되기 때문에 안전
- immutability : 불변의 데이터
- partition 가능
- location setup이 가능하다 (어떤 노드에 위치시킬지..?)
- sparkSQL, mllib 모두 rdd로 돌아감

⇒ 테이블 같은 개념으로 이해하면 좋음

### Spark 3의 특징

- spark2에 비해 17배 빠름
- python 2 deprecated
- binary file support (such as images)
- Delta Lake 서포트 (ACID) ⇒ data lake
- gpu instance support ⇒ ML 쪽 기능 강화
- kubernetes support 강화

## 아파치 스파크를 Docker를 이용해 설치

- `docker run -it --rm -p 8888:8888 -v /home/dycsnu/git/Apache_Spark_Inflearn/:/home/jovyan/work jupyter/pyspark-notebook`
- `docker ps` ⇒ container id 도출
- `docker exec -it 33362da5690e /bin/bash`
- SparkContext를 실행 시 Py4J 가 실행되고 JVM이 launch 된 후 JavaSparkContext를 생성한다
- `sc.parallelize` : 기존의 데이터셋을 RDD로 변환함

⇒ databricks community edition 사용해도 됨

- 클러스터 생성하고 노트북 생성하면 됨
- 15GB 메모리까지 가능

## Spark Architecture(스파크 아키텍쳐) 소개

- cluster란?
    - single system과 같이 행동하여 HA/LoadBalancing/Parallel Processing 가능

### YARN Runtime Architecture

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/02301e83-3332-4f3a-b283-2a8f081a0e2b/Untitled.png)

1. 클러스터의 Master Node에 Spark Job Submit
2. Master Node의 resource manager가 worker node의 Node Manager에게 Container를 시작하도록 명령함
3. 여러개의 Container 중 1개가 선택되어 Application Master가 생성됨
4. Application Master가 해당 Job 수행에 얼마의 Resource가 필요한지 판단하여 Master Node의 Resource Manager에게 전달
5. Resource Manager는 모든 노드의 리소스 정보를 갖고 있기 때문에 할당할 리소스를 Application Master에게 다시 전달함
6. Application Master가 할당받은 리소스에 대해 다른 Node Manager들에게 Container를 시작하도록 함
7. 시작된 Conatiner에서 Task가 할당되어 Job이 수행됨

⇒ 따라서 여러 Application이 실행되도 Resource가 관리됨!

### PySpark Runtime Architecture

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/49d86623-6c3c-4907-bd94-0d39b224535e/Untitled.png)

- 여러 개의 Container 중 하나가 선택되어 Spark Executor가 시작됩니다.
- Spark Executor가 Spark Driver와 통신하도록 설정됩니다.
- Spark Driver는 Spark Context를 생성합니다. 이때 Py4J가 실행되어 JVM이 시작됩니다.
    - **pyspark는 직접적으로 spark core에 접근이 되지 않기 때문에 JVM이 생성**
    - Py4J는 Python이 java application에 명령을 하도록 하는 라이브러리
- Spark Driver는 Application을 실행하고, Task를 생성하여 Spark Executor에게 전달합니다.

- Python Wrapper → Java Wrapper → Spark Core
    - 항상 Java Wrapper를 경유함
- 모든 worker node에도 python worker가 있는 이유?
    - spark library가 아닌 다른 python library를 사용하는 경우에 해당 코드를 execution할 interpretor가 필요하기 때문에!
    - 만약 dependency가 있는 external python library를 사용하지 않는 경우 python worker는 시작되지 않음
    - 만약 pandas / numpy를 사용하는 경우 모든 worker 노드들에 해당 패키지가 설치되어 잇어야 함 (아니면 에러 발생)
