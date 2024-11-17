import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal;

public class OrderStreamProcessor {
    public static void main(String args[]) {
        AmazonDynamoDBLocal amazonDynamoDBLocal = DynamoDBEmbedded.create();
        AmazonDynamoDBStreams dynamoDBStreams = amazonDynamoDBLocal.amazonDynamoDBStreams();

    }
}
