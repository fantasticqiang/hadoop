import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.net.InetAddress;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class ExportOracle {

    /*
     * 配置
     */
    //ES索引的名称
    private static String ES_INDICE = "payorder";
    //ES索引的类型
    private static String ES_TYPE = "payorder";
    //保存Oracle数据的文件位置
    private static String FILE_PATH = "D:/oracle.txt";

    private static Connection connection = null;
    private static PreparedStatement statement = null;

    static {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            connection = DriverManager.getConnection("jdbc:oracle:thin:@192.168.1.68:1521:orcl","pay_db","pay_db123");
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        TransportClient client = getEsConnection();
        //createIndice(client);//创建索引
        //indiceAddMapping(client,ES_INDICE,ES_TYPE);//添加mapping
        //deleteIndice(client);//删除索引
        excelToES(client,"C:\\Users\\lqq\\Desktop\\table_export.xls");

        client.close();
    }

    /**
     * 查询oracle数据,导入到文本中
     */
    public static void exportOracleToTxt () throws Exception {
        String sql = "select * from PAY_ORDER where CREATETIME > to_date('2017-05-01 00:00:00','yyyy-mm-dd hh24:mi:ss') and CREATETIME < to_date('2017-05-31 23:59:59','yyyy-mm-dd hh24:mi:ss')";
        statement = connection.prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery();
        List<String> columnLabels = getColumnLabels(resultSet);
        File file = new File(FILE_PATH);
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        while (resultSet.next()){
            String line = "";
            for (String columnLabel : columnLabels) {
                Object value = resultSet.getObject(columnLabel);
                line += value + ",";
            }
            line = line.substring(0,line.length()-1);
            writer.write(line);
        }
        writer.close();
        resultSet.close();
        statement.close();
    }

    /**
     * 获取列名
     * @param resultSet
     * @return
     * @throws SQLException
     */
    private static List<String> getColumnLabels(ResultSet resultSet)
            throws SQLException {
        List<String> labels = new ArrayList<String>();

        ResultSetMetaData metaData = (ResultSetMetaData) resultSet.getMetaData();
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            labels.add(metaData.getColumnLabel(i + 1));
        }
        return labels;
    }

    /**
     * 连接es,获得client
     */
    public static TransportClient getEsConnection() throws Exception {
        Settings settings = Settings.builder()
                .put("cluster.name", "test")
                .build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("47.94.193.202"), 9300));
        return client;
    }

    /**
     * 创建索引payorder
     */
    public static void createIndice (TransportClient client) {
        CreateIndexResponse createIndexResponse = client.admin().indices()
                .prepareCreate(ES_INDICE)
                .setSettings(Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0))
                .get();
        String result = createIndexResponse.toString();
        System.out.println("创建索引结果：" + result);
    }

    /**
     * 删除索引
     */
    public static void deleteIndice (TransportClient client) {
        DeleteIndexResponse response = client.admin().indices().prepareDelete(ES_INDICE)
                .execute().actionGet();
        System.out.println(response.toString());
    }

    /**
     * 索引添加mapping
     */
    public static void indiceAddMapping (TransportClient client, String index, String type) throws Exception {
        String mapping = "{\n";
        mapping += "\""+type+"\"" + ":{\n";
        mapping += "\"properties\":{\n";
        //获得列集合
        String sql = "select * from PAY_ORDER where rownum <= 1";
        statement = connection.prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery();
        List<String> columnLabels = getColumnLabels(resultSet);
        for (String column : columnLabels) {
            mapping += "\"" + column +"\":{";
            if("CREATETIME".equals(column)){
                mapping += "\"index\":\"not_analyzed\",\"type\":\"date\"}\n";
            }else if("PAYORDNO".equals(column)){
                mapping += "\"index\":\"not_analyzed\",\"type\":\"string\"}\n";
            }else{
                mapping += "\"index\":\"no\",\"type\":\"string\"}\n";
            }
            mapping += ",";
        }
        mapping = mapping.substring(0,mapping.length()-1);
        mapping += "}\n";
        mapping += "}\n";
        mapping += "}\n";
        PutMappingRequest request = Requests.putMappingRequest(index).source(mapping).type(type);
        client.admin().indices().putMapping(request).actionGet();
    }

    /**
     * 从excel导入es
     */
    public static void excelToES (TransportClient client, String excelFilePath) throws Exception {
        FileInputStream is = new FileInputStream(excelFilePath);
        Workbook workbook = new HSSFWorkbook(is);
        Sheet sheet = workbook.getSheetAt(0 );
        Row row = sheet.getRow(0);
        Iterator<Cell> iterator = row.cellIterator();
        List<String> columns = new ArrayList<String>();
        while (iterator.hasNext()) {
            Cell cell = iterator.next();
            String value = cell.getStringCellValue();
            if ("".equals(value)) {
                break;
            }
            columns.add(value);
        }
        int sendFlag = 0;
        int count = 0;
        List<String> data = new ArrayList<String>();
        for (int i = 1;i<sheet.getPhysicalNumberOfRows();i++) {
            Row rowx = sheet.getRow(i);
            String json = row2json(rowx,columns);
            sendFlag ++;
            data.add(json);
            if (sendFlag == 1000) {
                addJsonToEs(client,data);
                data.clear();
                sendFlag = 0;
            }
            count ++;
        }
        addJsonToEs(client,data);
        System.out.println("导入共 "+count+" 条");
    }
    /**
     * 从json数组添加到es
     */
    private static void addJsonToEs (TransportClient client, List<String> jsons) {
        BulkRequestBuilder bulk = client.prepareBulk();
        for (int i=0;i<jsons.size();i++) {
            bulk.add(client.prepareIndex(ES_INDICE,ES_TYPE).setSource(jsons.get(i)));
        }
        BulkResponse response = bulk.get();
        System.out.println("添加json至es："+response.buildFailureMessage());
    }
    /**
     * 根据cell的列获取对应的对象
     */
    private static String row2json (Row row,List<String> columns) {
        Iterator<Cell> cellIterator = row.cellIterator();
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String json = "{";
        int i = 0;
        while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            String value ;
            switch (i) {
                case 0 : {
                    value = cell.getStringCellValue();
                }
                case 7 : {
                    value = cell.getStringCellValue();
                }
                json += "\"" + columns.get(i) + "\":\"" + value + "\",";
            }
            i++;
        }
        json = json.substring(0,json.length()-1);
        json += "}";
        return json;
    }

}


