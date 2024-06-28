FROM flink:1.19.0

RUN mkdir /opt/flink/usrlib
ADD target/recommendations-*jar /opt/flink/usrlib/product-recommendations.jar
ADD data/productInventory.csv /opt/flink/usrlib/productInventory.csv