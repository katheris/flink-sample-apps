package flink.datasource;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class FileSourceDataGenerator {

    public static void generateProductData() throws IOException {
        List<String> productCategories = new ArrayList<>();
        productCategories.add("technology");
        productCategories.add("home&furniture");
        productCategories.add("garden");
        productCategories.add("appliances");
        productCategories.add("toys");
        productCategories.add("gaming");
        productCategories.add("clothing");
        productCategories.add("accessories");
        productCategories.add("health&beauty");
        productCategories.add("sports&leisure");

        Random random = new Random();

        String dataDir = "data/products.csv";
        BufferedReader br = new BufferedReader(new FileReader(dataDir));
        if (br.readLine() == null) {

            FileWriter productFile = new FileWriter(dataDir);
            for(int i=0; i < 200; i++) {
                String productCategory = productCategories.get(random.nextInt(productCategories.size()));
                productFile.write(i + "," + productCategory);
                productFile.write(System.getProperty("line.separator"));
            }
            productFile.close();
        }
    }
}
