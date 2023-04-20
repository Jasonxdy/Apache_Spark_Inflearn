# Apache Spark Introduction: 아파치 스파크 소개

## 아파치 스파크 소개

***Multi Language Engine** for executing **data engineering, data science, and manchine learning** on **single-node** macheines or **clusters***

- 기존 map reduce에 비해 빠름 (in-memory, disk i/o 줄임, 10~100배 빠름)
- python / java / scala
- RDD (resilient distributed dataset)!
    - 1개의 노드가 장애가 나도 사용 가능 (distributed)
    - 가장 중요한 컨셉

![image](https://user-images.githubusercontent.com/57127841/233251420-0e300327-5ae8-400b-83ef-19fe82bcd693.png)


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

![image](https://user-images.githubusercontent.com/57127841/233251475-bdb42a0e-880f-49ca-9b26-8beb5b504bbd.png)

1. 클러스터의 Master Node에 Spark Job Submit
2. Master Node의 resource manager가 worker node의 Node Manager에게 Container를 시작하도록 명령함
3. 여러개의 Container 중 1개가 선택되어 Application Master가 생성됨
4. Application Master가 해당 Job 수행에 얼마의 Resource가 필요한지 판단하여 Master Node의 Resource Manager에게 전달
5. Resource Manager는 모든 노드의 리소스 정보를 갖고 있기 때문에 할당할 리소스를 Application Master에게 다시 전달함
6. Application Master가 할당받은 리소스에 대해 다른 Node Manager들에게 Container를 시작하도록 함
7. 시작된 Conatiner에서 Task가 할당되어 Job이 수행됨

⇒ 따라서 여러 Application이 실행되도 Resource가 관리됨!

### PySpark Runtime Architecture

![image](https://user-images.githubusercontent.com/57127841/233251494-d0bb9fca-abc4-4502-a407-cee1f6e5e9c9.png)

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


# 아파치 스파크 RDD의 기본적인 특징과 예제

## Word Count 예제를 통해 기본 문법을 알아보기

- 코드
    
    ```python
    # 첫번째 예제
    import pyspark
    
    test_file = "file:///home/jovyan/work/sample/word.txt"
    # sc = pyspark.SparkContext('local[*]')
    sc = pyspark.SparkContext.getOrCreate();
    
    text_file = sc.textFile(test_file)
    counts = text_file.flatMap(lambda line: line.split(" ")) \
                 .map(lambda word: (word, 1)) \
                 .reduceByKey(lambda a, b: a + b)
    print(counts.collect())
    
    counts = text_file.flatMap(lambda line: line.split(" "))
    pair = counts.map(lambda word: (word, 1))
    print(pair.collect())
    
    # 두번째 예제
    import collections
    import pyspark
    
    test_file = "file:///home/jovyan/work/sample/grade.txt"
    # sc = pyspark.SparkContext('local[*]')
    sc = pyspark.SparkContext.getOrCreate();
    
    text_file = sc.textFile(test_file)
    
    grade = text_file.map(lambda line: line.split(" ")[1])
    
    # Return the count of each unique value in this RDD as a dictionary of (value, count) pairs.
    grade_count = grade.countByValue()
    
    for grade, count in sorted(grade_count.items(), key=lambda item: item[1], reverse=True):
        print(f"{grade}: {count}")
    ```
    

- `sc.textFile("path")`
    - text file의 한줄이 RDD의 한 값들이 됨 (as string)

## Key Value Pair / Average Example 평균값 구하기

- 코드
    
    ```python
    import pyspark
    
    sc = pyspark.SparkContext.getOrCreate();
    # Key / Value RDD
    
    # creating Key / Value RDD
    total_by_brand = rdd.map(lambda brand: (brand, 1))
    
    # reduceByKey(): Merge the values for each key using an associative and commutative reduce function.
    from operator import add
    rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
    sorted(rdd.reduceByKey(add).collect())
    # [('a', 2), ('b', 1)]
    
    # groupByKey(): Group the values for each key in the RDD into a single sequence. Hash-partitions the resulting RDD with numPartitions partitions.
    rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
    sorted(rdd.groupByKey().mapValues(len).collect())
    # [('a', 2), ('b', 1)]
    sorted(rdd.groupByKey().mapValues(list).collect())
    # [('a', [1, 1]), ('b', [1])]
    
    # sortByKey(): Sorts this RDD, which is assumed to consist of (key, value) pairs.
    tmp = [('a', 1), ('b', 2), ('1', 3), ('d', 4), ('2', 5)]
    sc.parallelize(tmp).sortByKey().first()
    # ('1', 3)
    
    # keys(), values(): Create a RDD of keys or just values
    rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
    rdd.keys()
    # ['a', 'b', 'a']
    
    # join, rightOuterJoin, leftOuterJoin, cogroup, subtractByKey
    x = sc.parallelize([("a", 1), ("b", 4)])
    y = sc.parallelize([("a", 2), ("a", 3)])
    sorted(x.join(y).collect())
    # [('a', (1, 2)), ('a', (1, 3))]
    
    # Efficiency is the key for performance!!!
    # if you only need values, use mapValues() or flatMapValues()
    
    import pyspark 
    
    sc = pyspark.SparkContext.getOrCreate();
    test_file = "file:///home/jovyan/work/sample/house_price.csv"
    
    def parse_line(line: str):
        city, price, count = line.split(',')
        return (int(price), int(count))
    
    lines = sc.textFile(test_file)
    price_count = lines.map(parse_line)
    # [(10000, 3), (10000, 5), (40000, 7), (5000, 7), (4000, 2), (9000, 4), (5000, 7), (4000, 2), (8000, 9)]
    
    sum_of_count = price_count.mapValues(lambda count: (count, 1))\
                    .reduceByKey(lambda a, b: (int(a[0]) + int(b[0]), int(a[1]) + int(b[1]))) 
    
    # ('10000', (3, 1)), ('10000', (5, 1)) ...
    # [('10000', (8, 2)), ('4000', (4, 2)), ('9000', ('4', 1)), ('8000', ('9', 1)), ('40000', ('7', 1)), ('5000', (14, 2))]
    
    avg_by_count = sum_of_count.mapValues(lambda total_count: int(total_count[0]) / total_count[1])
    results = avg_by_count.collect()
    print(results)
    ```
    

### Key-Value RDD
- 키-값 쌍이기 때문에 NoSQL Database와 같이 다룰 수 있다
- 첫번째 element는 키, 두번째 element는 value가 됨
    - `totalsByAge = rdd.map(lambda x : (x, 1))`
- 그럼 요소 3개인 tuple을 map fucntion에 넘기면 어떻게 될까? (feat chatgpt)
    
    ```
    what becomes the next rdd's key if you deliver a tuple with 3 elements to spark rdd map function?
    
    In Spark RDD map function, if you deliver a tuple with three elements, the first element of the tuple becomes the key of the next RDD. The second and third elements of the tuple become the values of the next RDD.
    
    For example, consider the following code snippet:
    
    rdd = sc.parallelize([(1, 2, 3), (4, 5, 6), (7, 8, 9)])
    new_rdd = rdd.map(lambda x: (x[0], (x[1], x[2])))
    
    In this code, we create an RDD called rdd with three tuples, each containing three elements. We then apply a map function that creates a new tuple with the first element as the key and a tuple of the second and third elements as the value. The resulting new_rdd will be:
    
    [(1, (2, 3)), (4, (5, 6)), (7, (8, 9))]
    
    In this case, **the first element of each tuple becomes the key of the next RDD, and the second and third elements become the values of the next RDD.** This allows us to easily perform further transformations or actions on the RDD, such as joining with another RDD on the key or filtering by a specific key.
    ```
    

### RDD functions

- `map` : 1:1로 매핑되어 Callable에 의해 RDD를 다음 RDD로 새로 생성해줌
    
    ```python
    rdd = sc.parallelize(["The quick red", "fox jumped"])
    rdd2 = rdd.map(lambda x: x.split())
    print(rdd2.collect())
    
    # [['The', 'quick', 'red'], ['fox', 'jumped']]
    ```
    
- `flatmap` : 1개의 input entry를 다수의 element로 만들어줌
    
    ```python
    rdda = sc.parallelize(["The quick red", "fox jumped"])
    rdda2 = rdd.flatMap(lambda x: x.split())
    print(rdda2.collect())
    
    # ['The', 'quick', 'red', 'fox', 'jumped']
    ```
    
- `reduceByKey` : 동일 key를 갖는 value에 대해 Callable의 값을 반환
    - `rdd.reduceBykey(lambda x, y: x + y)`
        - x, y : 동일 키를 갖는 두 value
        - **동일 키를 갖는 두 값에 대해 value를 더함**
- `groupByKey` : 동일 Key를 갖는 value를 grouping함
- `sortByKey` : key를 기준으로 RDD를 소팅
- `keys()`, `values()` : key 혹은 value만 갖는 RDD를 반환
- `mapValues`
    - `mapValues(lambda x : (x,1))`
        - **(33, 356) ⇒ (33, (356,1))로 변환함**
- key-value RDD 사용 시 value만을 사용해야 하는 경우 **map, flatMap 대신 mapValues, flatMapValues를 써야하는 이유?**
    - mapValues 및 flatMapValues가 데이터를 셔플하는 대신 **기존 RDD의 데이터 파티션을 유지**시켜주기 때문에 이점이 있음

### key-value RDD function (SQL Style)

- join, rightOuterJoin, leftOuterJoin, cogroup, subtractByKey, …

## pyspark library 설명 by code

`from pyspark import SparkConf, SparkContext`

- `SparkConf` : sparkcontext를 설정함
    - `conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")`
        - setMaster : local machine에서 돌릴 것으로 명시
        - setAppName : spark application name 설정, SparkUI에서 확인 시 용이
- `SparkContext` : RDD를 생성
    - `sc = SparkContext(conf = conf)`

- `ratings = lines.map(lambda x : x.split()[2])`
    - map : transformation function으로, 기존 RDD를 변형하는 것이 아니라 **새로운 RDD를 생성**한다
    - 따라서 ratings와 같은 새로운 변수로 할당을 해줘야 추가 동작이 가능 (단순히 map funciton 호출로는 아무 일도 안일어남)

## Filtering RDD

- `rdd.filter(Callable)`
    - callable (defined function 혹은 lambda function 등)을 모든 RDD의 요소에 적용하여 boolean 값이 True인 레코드만 반환함
    - `rdd.filter(lambda x : "TMIN" in x[1]")`
        - x[1]에 TMIN이 있는 경우만 filtering하고 나머지는 discard

# SparkSQL, DataFrames and DataSets

# DataFrames

- **Row 객체의 집합**
- SQL 쿼리 실행 가능
- schema를 갖는 것이 가능하다 (조금 더 효율적인 스토리지 관리)
- JSON / Hive / Parquet / CSV / … 읽는 것이 가능
- JDBC / ODBC, Tableau와 연동 가능 (communicate)

### DataFrame 생성 및 활용 기본

- `from pyspark.sql import SparkSession, Row`
    - **pyspark.sql 내 SparkSession에 SparkSQL 있음**
- `spark = SparkSession.builder.appName("SparkSQL").getOrCreate()`
    - **SparkSession 클래스의 builder를 사용하여 세션을 생성할 수 있음**
- `inputData = spark.read.json(dataFile)`
    - spark.read로 파일 읽기
- `inputData.createOrReplaceTempView("myStructuredStuff")`
    - 테이블 생성
- `myResultDataFrame = spark.sql("SELECT foo FROM bar ORDER BY foobar")`
    - **spark.sql로 쿼리 가능**
- `myResultDataFrmae.show()`
    - dataframe 레코드들 확인
- `myResultDataFrmae.select("col_name")`
    - Dataframe의 select 함수로 컬럼 선택도 가능
- `myResultDataFrmae.filter(myResultDataFrame("col_name">200))`
    - where 조건 처럼 필터 가능
- `myResultDataFrmae.groupBy(myResultDataFrame("col_name")).mean()`
    - groupby 후 aggregation function 호출 가능
- `myResultDataFrmae.rdd().map(mapperFunction)`
    - rdd 함수로 dataframe ⇒ **rdd로 변환 및 map 함수 사용** 가능
- `SparkContext.textFile("path")`로 받아와서 RDD → Dataframe으로 만들기

```python
# Row object를 생성하는 함수를 RDD에 map으로 전달
def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile("fakefriends.csv")
**people = lines.map(mapper)**

# 이후 SparkSession.createDataFrame으로 데이터프레임화
schemaPeople = spark.createDataFrame(people).cache() # cache => RDD를 스파크 잡 동안 유지
# persist vs cache 차이 인지 (https://jaemunbro.medium.com/apache-spark-rdd-%EC%9E%AC%EC%82%AC%EC%9A%A9%EC%9D%84-%EC%9C%84%ED%95%9C-%EC%98%81%EC%86%8D%ED%99%94-persist-cache-checkpoint-12c121dac8b6)
schemaPeople.createOrReplaceTempView("people")
```

### RDD 대신 DataFrame 사용하기

- SparkSession 생성 후 `spark.read.option` 활용
    - `("header", "true)` : 헤더 있는 경우 읽음
    - `("inferSchema", "true)` : 파일을 읽어서 데이터 기반으로 스키마를 유추ㅜ함
- `.csv("path")` 로 csv 읽음
- `df.column_name` 이런식으로 컬럼 선택도 가능 ⇒ pandas와 동일
- `agg` 함수로 transformation 시 컬럼명 지정은 alias 함수로!
    - `.alias("col_name"))`

### DataFrame 함수

- `withColumn` : 컬럼을 새로 정의하거나 기존 컬럼값을 변경할때 사용
    - `df.withColumn("new_col_name", df.old_col_name + 10)`

### SQL function 활용하기(`from pyspark.sql import function`)

- round, avg 등의 함수로 처리 시 `from pyspark.sql import function as func`로 import해서 사용할 것 ⇒ python 내장 round 사용 시 에러 발생 가능
    - `df.groupBy("colname").agg(func.round(func.avg("col_name_2"),2))`
- `lower`
    - lower case로 변환
- `explode`
    - 모든 행을 풀어버림 (flatMap과 동일 기능)
- `spark.read.text("unstructured_text_path")`
    - 이렇게 불러올 시 default 컬럼명이 `value`인 dataframe 생성 (컬럼명이 value 인 row 객체의 집합)
    - `df.value`로 호출 가능

### DataFrame에 explicit schema 생성하기

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

schema = StructType([ \
                     StructField("stationID", StringType(), True), \
                     StructField("date", IntegerType(), True), \
                     StructField("measure_type", StringType(), True), \
                     StructField("temperature", FloatType(), True)])

df = spark.read.schema(schema).csv("file:///SparkCourse/1800.csv")
```

- pyspark.sql.types에서 import
- `StructType` 객체로 스키마 정의 : `StructField`의 리스트를 전달
- `StructField` : 컬럼명 / 타입 / nullable 정의
- SparkSession의 `spark.read.schema` 함수의 파라미터로 전달

### DataSet?

- scala는 type이 정해진 언어이고 DataSet은 typed data를 storing하는 것이 가능하다
    - typed되면 저장이 조금 더 효율적이고 compile 시 이점이 있을 수 있음
- 따라서 python은 dataframe, scala는 dataset을 주로 사용

# Spark 고급 예제

## Broadcast 변수 사용

- 만약 customer_id와 customer_name이 각기 다른 데이터셋에 존재한다면?
    1. 두 데이터셋을 올려서 join하기
        1. 이는 굉장히 spark oriented way긴 하지만 **데이터가 크지 않다면 shuffling 등의 불필요한 overhead가 발생**할 수 있다
    2. 데이터가 크지 않을 때의 다른 대안으로는 spark driver program에 dictionary (ex. customer_id : customer_name) 를 올려서 필요할 때 각 executor에 한번 전달하여 거기에 체류하도록 할 수 있음 ⇒ **broadcast 변수**

- Broadcast object는 SparkContext에 전달하면 각 executor에 전달하고 이는 executor에서 가지고 있게 됨
- 사용법
    - `broadcast_object = spark.SparkContext.broadcast(object)`
        - object에는 dictionary를 return하는 함수를 사용하면 됨
        - SparkSession이 아닌 SparkContext에 있음에 유의
    - `broadcast_object.value`
        - broadcasting된 dictionary 반환
