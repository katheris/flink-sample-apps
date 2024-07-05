# Recommendation system use case
The sample scenario:
Orinoco Inc, retail sales company wants to display a widget on product pages of similar products that the user might be interested in buying.
They should recommend:
- Up to 6 highly rated products in the same category as that of the product the customer is currently viewing
- Only products that are in stock
- Products that the customer has bought before should be favoured
- <i>TODO: Avoid showing suggestions that have already been made in previous pageviews </i>

The data:
- Input: A clickstream (user id, product id, event time)
- Input: A stream of purchases (user id, product id, purchase date)
- Input: An inventory of products (product id, product name, product category, number in stock, rating)

Output: A stream of recommendations (user id, 6 product ids)

# Running recommendation app

This is how I ran the app with Strimzi using the [Flink Kubernetes Operator SQL example](https://github.com/apache/flink-kubernetes-operator/tree/main/examples/flink-sql-runner-example). 

The SqlRunner class is modified to generate some sample data for the input topics, `flink.click.streams` and `flink.sales.records` and to create the output topic, `flink.recommended.products`. The output is produced to a compacted topic, keyed by `userId` value. The data for the product inventory and SQL script are copied to the local directory when building a container image for `FlinkDeployment`. 

These steps are for runnig a Flink job based on the SQL statements with Strimzi in minikube:

1. Start minikube with the following resources.

   ```
   MINIKUBE_CPUS=4
   MINIKUBE_MEMORY=16384
   MINIKUBE_DISK_SIZE=25GB
   ```

2. Create a `flink` namespace:
   ```
   kubectl create namespace flink
   ```

3. Apply the Strimzi QuickStart:
   ```
   kubectl create -f 'https://strimzi.io/install/latest?namespace=flink' -n flink
   ```
4. Create a Kafka
   ```
   kubectl apply -f kafka.yaml -n flink
   ```
5. Install cert-manager (this creates cert-manager in a namespace called `cert-manager`):
   ```
   kubectl create -f https://github.com/jetstack/cert-manager/releases/download/v1.8.2/cert-manager.yaml
   ```
6. Deploy Flink Kubernetes Operator 1.8.0 (the latest stable version):
   ```
    helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.8.0/
    helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator -n flink
   ```
7. Build the image like this:
   ```
   mvn clean package && minikube image build data-generator -t data-generator:latest && minikube image build sql-runner -t recommendation-app:latest
   ```

8. Create the data generator Kubernetes Deployment:
   ```
   kubectl apply -f data-generator.yaml -n flink
   ```

9. Create the `FlinkDeployment`:
   ```
   kubectl apply -f recommendation-app.yaml -n flink
   ```
10. In a separate tab, `exec` into the kafka pod and run the console consumer:
    ```
    kubectl exec -it my-cluster-dual-role-0 -n flink -- /bin/bash \
    ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic flink.recommended.products --from-beginning
    ```
11. You should see messages such as the following:
   ```
    user-27,"140,13,137,95,39,138","2024-06-28 13:01:55"
    user-14,"40,146,74,81,37,19","2024-06-28 13:01:55"
    user-36,"42,106,82,153,158,85","2024-06-28 13:02:00"
    user-5,"83,123,77,41,193,136","2024-06-28 13:02:00"
    user-27,"55,77,168","2024-06-28 13:02:05"
    user-44,"140,95,166,134,199,180","2024-06-28 13:02:10"
    user-15,"26,171,1,190,87,32","2024-06-28 13:02:10"
   ```
   The expected format of the result is `userId`, `comma separated 6 product ids` and `timestamp` of the window.
