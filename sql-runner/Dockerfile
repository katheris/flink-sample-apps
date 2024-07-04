FROM flink:1.19.0

RUN mkdir /opt/flink/usrlib
ADD target/sql-runner-*.jar /opt/flink/usrlib/sql-runner.jar
ADD data/productInventory.csv /opt/flink/data/productInventory.csv
ADD sql-scripts /opt/flink/usrlib/sql-scripts