package nearsoft.academy.bigdata.recommendation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class MovieRecommender {

    private String resource_Path;

    private int totalReviews;
    private int totalProducts;
    private int totalUsers;

    private BiMap<String,Integer> productHash;
    private BiMap<String,Integer> userHash;

    UserSimilarity similarity;
    UserNeighborhood neighborhood;
    UserBasedRecommender recommender;

    MovieRecommender(String resource_Path) throws IOException {
        this.resource_Path = resource_Path;
        this.totalReviews = 0;
        this.totalProducts = 0;
        this.totalUsers = 0;

        this.productHash = HashBiMap.create();
        this.userHash = HashBiMap.create();
        
        readFile(resource_Path);
    }

    public String getResource_Path() {
        return resource_Path;
    }

    public void setResource_Path(String resource_Path) {
        this.resource_Path = resource_Path;
    }

    public int getTotalReviews() {
        return totalReviews;
    }

    public int getTotalProducts() {
        return totalProducts;
    }

    public int getTotalUsers() {
        return totalUsers;
    }

    public List<String> getRecommendationsForUser(String user) throws IOException, TasteException{
        DataModel model = new FileDataModel(new File("resultado.csv"));

        this.similarity = new PearsonCorrelationSimilarity(model);
        this.neighborhood = new ThresholdUserNeighborhood(0.1, similarity, model);
        this.recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);
        
        int userId = this.userHash.get(user);

        List<RecommendedItem> recommended = recommender.recommend(userId, 3);
        List<String> recommendations = new ArrayList<String>();

        BiMap<Integer, String> productHashInverse = this.productHash.inverse();

        for(RecommendedItem recommendation: recommended){
            recommendations.add(productHashInverse.get((int)recommendation.getItemID()));
        }

        return recommendations;
    }

    private void readFile(String path)throws IOException{
        int currentProduct = 0;
        int currentUser = 0;

        String line;

        File result = new File("resultado.csv");
        FileWriter fw = new FileWriter(result);

        InputStream stream = new GZIPInputStream(new FileInputStream(path));
        BufferedReader br = new BufferedReader(new InputStreamReader(stream));
        
        while((line=br.readLine()) != null){

            if(line.startsWith("product/productId:")){
                String productID = line.split(" ")[1];
                
                if(this.productHash.containsKey(productID) != true){
                    this.totalProducts++;
                    this.productHash.put(productID, this.totalProducts);
                    currentProduct = this.totalProducts;
                }else currentProduct = this.productHash.get(productID);
                continue;
            }

            if(line.startsWith("review/userId")){
                String userID = line.split(" ")[1];

                if(this.userHash.containsKey(userID) != true){
                    this.totalUsers++;
                    this.userHash.put(userID, this.totalUsers);
                    currentUser = this.totalUsers;
                }else currentUser = this.userHash.get(userID);
                continue;
            }

            if(line.startsWith("review/score")){
                String score = line.split(" ")[1];
                fw.write(currentUser + "  " + currentProduct + "  " + score + "\n");
                this.totalReviews++;
            }
        }
        fw.close();
        br.close();
    }

}
