package sxq.demo;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;
import org.elasticsearch.common.text.Text;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Created by Randy on 2017-09-06.
 */
public class ElasticSearchTest {

    TransportClient client;
    Connection connection ;

    private static AtomicLong currentSynCount = new AtomicLong(0L); // 当前已同步的条数
    private static int syncThreadNum = 10; // 同步的线程数

    @Before
    @SuppressWarnings({"unchecked"})
    public void before() throws UnknownHostException, InterruptedException, ExecutionException, ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        connection = DriverManager.getConnection("jdbc:mysql://192.168.1.12:3306/uhome", "uhome", "uhome110");
        Settings esSettings = Settings.builder()
                .put("cluster.name", "my-application") //设置ES实例的名称
                .put("client.transport.sniff", true) //自动嗅探整个集群的状态，把集群中其他ES节点的ip添加到本地的客户端列表中
                .build();
        client = new PreBuiltTransportClient(esSettings);//初始化client较老版本发生了变化，此方法有几个重载方法，初始化插件等。
        //此步骤添加IP，至少一个，其实一个就够了，因为添加了自动嗅探配置
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
    }

    public class Tt extends  Thread {
        private Integer initVal;
        private Integer limit;
        public Tt(Integer initVal,Integer limit){
            this.initVal=initVal;
            this.limit=limit;
        }
        @Override
        public void run() {

            try{
                PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM CUSTOMER   limit "+initVal+" , "+limit);
                ResultSet resultSet = preparedStatement.executeQuery();
                while( resultSet.next()){
                    currentSynCount.incrementAndGet();// 递增
                    Map<String, Object> infoMap = new HashMap<String, Object>();
                    infoMap.put("custId",resultSet.getString("cust_id"));
                    infoMap.put("custName",resultSet.getString("cust_name"));
                    infoMap.put("custPhone",resultSet.getString("cust_phone"));
                    infoMap.put("address",resultSet.getString("address"));
                    IndexResponse indexResponse = client.prepareIndex("uhome", "customer", resultSet.getString("cust_id")).setSource(infoMap).execute().actionGet();
                    currentSynCount.incrementAndGet();
                    System.out.println(Thread.currentThread().getName()+"\tid:" + indexResponse.getId());
                }
            } catch(Exception e){
                e.printStackTrace();
            }
        }
    }
    @Test
    public void index() throws Exception {
        int totalRecord=0;
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT count(1) FROM CUSTOMER ");
        ResultSet resultSet = preparedStatement.executeQuery();
        while( resultSet.next()){
            totalRecord=resultSet.getInt(1);
        }
        int threadCount=totalRecord/10000;
        for (int i = 0; i < threadCount; i++) {
            new Thread(new Tt(i*10000,10000),"线程"+i).start();
        }
        while (true);

        /*for (int i = 1; i < 200; i++) {
            Map<String, Object> infoMap = new HashMap<String, Object>();
            infoMap.put("name", "我是小"+i);
            infoMap.put("title", "小"+i);
            infoMap.put("createTime", new Date());
            infoMap.put("count", 200+i);
            IndexResponse indexResponse = client.prepareIndex("test", "info", "100"+i).setSource(infoMap).execute().actionGet();
            System.out.println("id:" + indexResponse.getId());
        }*/
    }

    //@Test
    public void get() throws Exception {
        GetResponse response = client.prepareGet("test", "info", "1001")
                .execute().actionGet();
        System.out.println("response:getVersion:" + response.getVersion());
        System.out.println("response.getId():" + response.getId());
        System.out.println("response.getSourceAsString():" + response.getSourceAsString());

    }

    //@Test
    public void delete(){
        DeleteResponse deleteResponse = client.prepareDelete("test", "info", "1002").execute().actionGet();
        System.out.println(deleteResponse);
    }
    //@Test
    public void query() throws Exception {
        //term查询
//        QueryBuilder queryBuilder = QueryBuilders.termQuery("age", 50) ;

        //match查询
        QueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("custName","齐星成");

        //设置hit高亮
        HighlightBuilder hiBuilder=new HighlightBuilder();
        hiBuilder.preTags("<h2>");
        hiBuilder.postTags("</h2>");
        hiBuilder.field("custName");

        //range查询
        //QueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("count").gte(208);
        SearchResponse searchResponse = client.prepareSearch("uhome")
                .setTypes("customer")
                .setQuery(matchQueryBuilder)
                //.addSort("custId", SortOrder.DESC)
                .setSize(100).highlighter(hiBuilder)
                .execute()
                .actionGet();
        SearchHits hits = searchResponse.getHits();
        System.out.println("ElasticSearch 为您找到相关结果约"+hits.getTotalHits()+"个!最高分:"+hits.getMaxScore());
        //遍历结果
        for(SearchHit hit:hits){
            String custName = (String) hit.getSource().get("custName");
            String custPhone = (String) hit.getSource().get("custPhone");
            System.out.format("custName:%s ,custPhone :%s ,score:%s\n", custName, custPhone,hit.getScore());
            /*System.out.println("String方式打印文档搜索内容:");
            System.out.println(hit.getSourceAsString());
            System.out.println("Map方式打印高亮内容");
            System.out.println(hit.getHighlightFields());*/
            System.out.println("遍历高亮集合，打印高亮片段:");

            Text[] text = hit.getHighlightFields().get("custName").getFragments();
            for (Text str : text) {
                System.out.println(str.string());
            }
            System.out.println("\n\n\n\n");
        }


        /*System.out.println("查到记录数：" + hits.getTotalHits());
        SearchHit[] searchHists = hits.getHits();
        if (searchHists.length > 0) {
            for (SearchHit hit : searchHists) {
                String name = (String) hit.getSource().get("name");
                String title = (String) hit.getSource().get("title");
                System.out.format("name:%s ,title :%s \n", name, title);
            }
        }*/
    }

}
