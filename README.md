# trino-verifier-queries

Handcrafted query suites for Trino verifier

### How to use

- Provision MySQL database (can be done with Docker for local testing)
    ```
    # docker run --name some-mysql -e MYSQL_ROOT_PASSWORD=password -p 3306:3306 -d mysql:8.0.28
    # docker exec -it some-mysql bash
    # mysql -uroot -ppassword
    > CREATE DATABASE verifier;
    > CREATE USER 'myuser'@'%' IDENTIFIED BY 'mypass';
    > GRANT ALL PRIVILEGES ON verifier . * TO 'myuser'@'%';
    ```
  Start existing MySQL instance
    ```
    # docker start some-mysql
    ```

- Create verifier table: https://github.com/trinodb/trino/blob/master/service/trino-verifier/README.md

- Create schemas (execute from the project root directory)
    ```
    
    ./target/trino-verifier-queries-*-executable.jar schemas --config=etc/schemas.properties create --catalog=hive --schema=tpch_tiny --scale-factor=0.01 --overwrite --threads 10 --schema-type tpch

    ./target/trino-verifier-queries-*-executable.jar schemas --config=etc/schemas.properties create --catalog=hive --schema=tpch_tiny_bucketed --scale-factor=0.01 --overwrite --threads 10 --bucket-count=9 --schema-type tpch

    ./target/trino-verifier-queries-*-executable.jar schemas --config=etc/schemas.properties create --catalog=hive --schema=tpcds_tiny --scale-factor=0.01 --overwrite --threads 10 --schema-type tpcds
  
    ./target/trino-verifier-queries-*-executable.jar schemas --config=etc/schemas.properties create --catalog=hive --schema=tpcds_tiny_partitioned --scale-factor=0.01 --overwrite --threads 10 --partitioned --schema-type tpcds
    ```

- Create verifier suites (execute from the project root directory)
    ```
    ./target/trino-verifier-queries-*-executable.jar suites --config=etc/suites.properties create --catalog hive --schema tpch_tiny --name tpch_tiny --control tiny --overwrite --suite tpch/standard

    ./target/trino-verifier-queries-*-executable.jar suites --config=etc/suites.properties create --catalog hive --schema tpch_tiny_bucketed --name tpch_tiny_bucketed_etl --control tiny --overwrite --suite tpch/etl

    ./target/trino-verifier-queries-*-executable.jar suites --config=etc/suites.properties create --catalog hive --schema tpcds_tiny --name tpcds_tiny --control tiny --overwrite --suite tpcds/standard
  
    ./target/trino-verifier-queries-*-executable.jar suites --config=etc/suites.properties create --catalog hive --schema tpcds_tiny --name tpcds_tiny_etl --control tiny --overwrite --suite tpcds/etl
    ```
- Run verifier (execute from the project root directory)
    ```
    java -Dsuites=tpch_tiny,tpch_tiny_bucketed_etl,tpcds_tiny,tpcds_tiny_etl -jar ../trino/service/trino-verifier/target/trino-verifier-*-executable.jar ./etc/verifier.properties
    ```
