package flink;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ProductDataFileGenerator {

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

        String dataDir = "data/productInventory.csv";
        BufferedReader br = new BufferedReader(new FileReader(dataDir));
        if (br.readLine() == null) {
            FileWriter productFile = new FileWriter(dataDir);
            for(int i=0; i < 200; i++) {
                String stock = String.valueOf(Math.abs(random.nextInt(100)));
                String rating = String.valueOf(Math.abs(random.nextInt(10) + 1));
                String productCategory = productCategories.get(random.nextInt(productCategories.size()));
                productFile.write(i + "," + productCategory + "," + stock + "," + rating);
                productFile.write(System.getProperty("line.separator"));
            }
            productFile.close();
        }
    }
}
