import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class EsClient {

    public static String ES_INDEX = "pay_order";
    public static String ES_TYPE = "pay_order";

    public static void main(String[] args) throws Exception {
        Settings settings = Settings.builder()
                .put("cluster.name", "test")
                .build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("47.94.193.202"), 9300));
        search(client);
    }

    /**
     * es 查询
     */
    public static void search(TransportClient client) throws Exception {
        SearchResponse response = client.prepareSearch("bank").get();
        SearchHits hits = response.getHits();
        for(SearchHit hit : hits){
            System.out.println(hit.getSource());
        }
        System.out.println("总条数："+hits.totalHits());
    }

    /**
     * 创建索引
     */
    public static void createIndex(TransportClient client){
        CreateIndexResponse createIndexResponse = client.admin().indices()
                .prepareCreate(ES_INDEX)
                .setSettings(Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0))
                .get();
        System.out.println("创建索引: "+createIndexResponse.toString());
    }
}
