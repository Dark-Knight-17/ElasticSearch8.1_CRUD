
package ElasticsearchAssignment;
/*

CRUD USING Elasticsearch JAVA API CLIENT 8.1

*/
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

public class App {

    public final static ObjectMapper mapper = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    final static RestClientBuilder builder = RestClient.builder(HttpHost.create("http://localhost:9200"));
    private static RestClient restClient;
    private static ElasticsearchTransport transport;
    static ObjectMapper jsonParser = new ObjectMapper();

    static ElasticsearchClient getClient() {

        restClient = builder.build();
        transport = new RestClientTransport(restClient,
                new JacksonJsonpMapper(mapper));
        return new ElasticsearchClient(transport);
    }

    static boolean create(String indexName, String theID, Map details) {
        boolean createdSuccesfully = false;

        ElasticsearchClient client = getClient();
        try {
            String json = mapper.writeValueAsString(details);
            JsonNode actualObj = jsonParser.readTree(json);
            client.index(b -> b
                    .index(indexName)
                    .id(theID)
                    .document(actualObj));

            GetResponse<Object> response = client.get(b -> b.index(indexName)
                    .id(theID), Object.class);
            System.out.println("\n\nItem Created/Updated With ID : " + response.id()
                    + "\nDetails : " + response.source());
            if (response.found() == true) {
                createdSuccesfully = true;
            }

        } catch (Exception ex) {
            Logger.getLogger(App.class.getName()).log(Level.SEVERE, null, ex);
        }
        return createdSuccesfully;
    }

    static boolean read(String indexName, String theID) {

        boolean readSuccesfully = false;
        ElasticsearchClient client = getClient();
        try {
            GetResponse<Object> response
                    = client.get(b -> b.index(indexName)
                    .id(theID), Object.class);
            if (response.source() == null) {
                System.out.println(theID + " ID does not Exists");
                return false;
            } else if (response.source() != null) {
                System.out.println("\n\nItem Found With ID : " + response.id()
                        + "\nDetails : " + response.source());
                readSuccesfully = true;
            }
        } catch (co.elastic.clients.elasticsearch._types.ElasticsearchException no_index) {
            System.out.println(indexName + " Does not exists");
            Logger.getLogger(App.class.getName()).log(Level.SEVERE, null, no_index);
        } catch (IOException ex) {
            Logger.getLogger(App.class.getName()).log(Level.SEVERE, null, ex);
        }
        return readSuccesfully;
    }

    static boolean update(String indexName, String theID, Map details) {
        boolean updatedSuccesfully = false;
        ElasticsearchClient client = getClient();
        try {
            boolean exists = read(indexName, theID);
            if (exists != true) {

                System.out.println("Given ID or index does not exists , so cannot update ");
                return updatedSuccesfully;
            } else {
                System.out.println("\nUpdating given index with given values");
                boolean updated = create(indexName, theID, details);
                System.out.println("\nReading the New values using read function");
                read(indexName, theID);
                updatedSuccesfully = updated;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return updatedSuccesfully;
    }

    static void delete(String toDeleteIndex) {
        try {
            ElasticsearchClient client = getClient();
            client.indices().delete(b -> b.index(toDeleteIndex));
        } catch (IOException ex) {
            Logger.getLogger(App.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ElasticsearchException ex) {
            Logger.getLogger(App.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void main(String[] args) throws IOException {

        String developerIndex = "developer_17_index";
        String developerIndex1 = "developer_17_index_test";
        String index = "index_for_test";
        String index2 = "index_for_test2";
        Map<String, String> details = new HashMap();
        Map<String, String> updatedDetails = new HashMap();

        details.put("Name ", " Developer 17 Test on THursday");
        details.put("Phone ", " 17171717");
        details.put("Hobbies ", " Playing Sports");
        details.put("Profession  ", " Cricketer");
        details.put("Email ", " developer17@email.com");
        
        
        updatedDetails.put("Profession  ", " Sdsdsdsdsdsdsdsdsdnger");
        updatedDetails.put("Email ", " singer7@email.com");
        String newIndex = "new_index";

        try {
//            boolean created=create(developerIndex1,"id1234",details);
//            boolean successfullyRead=read(developerIndex1, "id1234");
//            boolean successfullyRead2=read("09090", "idforu0"); // just to check if updating index isn't updating other node ids
//            boolean updated=update(developerIndex1, "id1234",updatedDetails);
//            System.out.println("updated "+updated);
            delete("09090");
//            System.out.println("deleted ");

        } catch (Exception e1) {
            e1.printStackTrace();

        } finally {

            restClient.close();
            transport.close();

        }

    }
}
