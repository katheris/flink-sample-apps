FROM flink:1.19.0

RUN mkdir /opt/flink/usrlib
ADD target/realtime-analytics-*jar /opt/flink/usrlib/realtime-analytics.jar
ADD data/productInventory.csv /opt/flink/usrlib/productInventory.csv