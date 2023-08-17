package com.aliyun.gts.slssniffer;

import com.alibaba.fastjson.JSON;
import com.aliyun.auth.credentials.Credential;
import com.aliyun.auth.credentials.provider.StaticCredentialProvider;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.request.ListShardRequest;
import com.aliyun.openservices.log.response.ListShardResponse;
import com.aliyun.openservices.loghub.client.ClientWorker;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.sdk.service.sls20201230.AsyncClient;
import com.aliyun.sdk.service.sls20201230.models.DeleteConsumerGroupRequest;
import com.aliyun.sdk.service.sls20201230.models.DeleteConsumerGroupResponse;
import com.aliyun.sdk.service.sls20201230.models.ListConsumerGroupRequest;
import com.aliyun.sdk.service.sls20201230.models.ListConsumerGroupResponse;
import com.google.gson.Gson;
import darabonba.core.client.ClientOverrideConfiguration;
import org.apache.commons.cli.*;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class SlsSniffer {
    // 日志服务域名，请您根据实际情况填写。更多信息，请参见服务入口。
    public static String sEndpoint = "";
    // 日志服务项目名称，请您根据实际情况填写。请从已创建项目中获取项目名称。
    public static String sProject = "";
    // 日志库名称，请您根据实际情况填写。请从已创建日志库中获取日志库名称。
    public static String sLogstore = "";
    // 消费组名称，请您根据实际情况填写。您无需提前创建，该程序运行时会自动创建该消费组。
    public static String sConsumerGroup = "slssniffer_consumer_group";
    // 消费数据的用户AccessKey ID和AccessKey Secret信息，请您根据实际情况填写。更多信息，请参见访问密钥。
    public static String sAccessKeyId = "";
    public static String sAccessKey = "";
    public static String outputFile="./out.json";
    public static long endTime=0l;
    public static long  startTime=0l;
    public static int threads=4;
    public static String logType="default";
    public static boolean sortByDate=false; //是否根据实际的SQL日期进行排序，严格确保回放顺序
    private static  Map<String,ClientWorker> arr =new HashMap<>();
    private static  ArrayList<Thread> threadArr =new ArrayList<>();
    public static boolean stop=false;
    public static long curTimestamp=System.currentTimeMillis();
    private static String region="cn-hangzhou";
    private static boolean deleteConsumerGroup=false;
    public static HashMap<String,String> filterMap=null;

    public static void updateCheckpoint() throws Exception {
        Client client = new Client(sEndpoint, sAccessKeyId, sAccessKey);
        ListShardResponse response = client.ListShard(new ListShardRequest(sProject, sLogstore));
            for (Shard shard : response.GetShards()) {
            int shardId = shard.GetShardId();
            String cursor = client.GetCursor(sProject, sLogstore, shardId, startTime).GetCursor();
            try{
                client.UpdateCheckPoint(sProject, sLogstore, sConsumerGroup, shardId, cursor);
            }catch (Exception e){
                if(!e.getMessage().contains("consumer group not exist")){
                    throw e;
                }
            }

        }
    }
    public static List<Shard> getShards() throws Exception {
        Client client = new Client(sEndpoint, sAccessKeyId, sAccessKey);
        ListShardResponse response = client.ListShard(new ListShardRequest(sProject, sLogstore));
        ArrayList<Shard> shardList=new ArrayList<>();
        for (Shard shard : response.GetShards()) {
            shardList.add(shard);
        }
        return shardList;
    }



    public static void main(String[] args) throws Exception {

        // consumer_1是消费者名称，同一个消费组下面的消费者名称必须不同，不同的消费者名称在多台机器上启动多个进程，来均衡消费一个Logstore，此时消费者名称可以使用机器IP地址来区分。
        // maxFetchLogGroupSize用于设置每次从服务端获取的LogGroup最大数目，使用默认值即可。您可以使用config.setMaxFetchLogGroupSize(100);调整，请注意取值范围为(0,1000]。
        initArgs(args);
        curTimestamp=startTime*1000;
        boolean consumerGroupExist=false;
        //判断消费组是否存在
        try{
            StaticCredentialProvider provider = StaticCredentialProvider.create(Credential.builder()
                    .accessKeyId(sAccessKeyId)
                    .accessKeySecret(sAccessKey)
                    .build());

            // Configure the Client
            AsyncClient client = AsyncClient.builder()
                    .region(region) // Region ID
                    //.httpClient(httpClient) // Use the configured HttpClient, otherwise use the default HttpClient (Apache HttpClient)
                    .credentialsProvider(provider)
                    //.serviceConfiguration(Configuration.create()) // Service-level configuration
                    // Client-level configuration rewrite, can set Endpoint, Http request parameters, etc.
                    .overrideConfiguration(
                            ClientOverrideConfiguration.create()
                                    .setEndpointOverride(sEndpoint)
                            //.setConnectTimeout(Duration.ofSeconds(30))
                    )
                    .build();

            // Parameter settings for API request
            ListConsumerGroupRequest listConsumerGroupRequest = ListConsumerGroupRequest.builder()
                    .project(sProject)
                    .logstore(sLogstore)
                    // Request-level configuration rewrite, can set Http request parameters, etc.
                    // .requestConfiguration(RequestConfiguration.create().setHttpHeaders(new HttpHeaders()))
                    .build();

            // Asynchronously get the return value of the API request
            CompletableFuture<ListConsumerGroupResponse> response = client.listConsumerGroup(listConsumerGroupRequest);
            // Synchronously get the return value of the API request
            ListConsumerGroupResponse resp = response.get();
            if(resp.getBody().size()>0){
                consumerGroupExist=true;
            }
            // Finally, close the client
            client.close();
        }catch (Exception e){
            e.printStackTrace();
            System.exit(1);
        }
        if(consumerGroupExist){
            updateCheckpoint();
        }

        for(int i=0;i<threads;i++){
            LogHubConfig config = new LogHubConfig(sConsumerGroup, "consumer_"+i, sEndpoint, sProject, sLogstore, sAccessKeyId, sAccessKey, (int)startTime,1000);
            ClientWorker worker = new ClientWorker(new SampleLogHubProcessorFactory(), config);
            String name="worker_"+i;
            Thread thread = new Thread(worker,name);
            //Thread运行之后，ClientWorker会自动运行，ClientWorker扩展了Runnable接口。
            thread.start();
            arr.put(name,worker);
            threadArr.add(thread);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                for(ClientWorker worker:arr.values())
                {
                    worker.shutdown();
                }
                stop=true;
            }
        });
        Timer monitorTimer=new Timer();
        //检查sql是否跑完，如果运行完毕，那么终止任务
        monitorTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                System.out.println("current seek time："+DateUtil.toChar(curTimestamp));
            }
        }, 0, 5000);

//        while(!stop){
//            int flag=0;
//            for (String key:threadCurrentTS.keySet()){
//                if (threadCurrentTS.get(key)>endTime){
//                    flag++;
//                }
//            }
//            try{
//                Thread.sleep(1000);
//            }catch (Exception e ){
//                e.printStackTrace();
//            }
//            if(flag==threadArr.size()){
//                System.out.println(JSON.toJSONString(threadCurrentTS));
//                stop=true;
//                //等待5秒，避免丢失日志
//                try{
//                    Thread.sleep(5000);
//                }catch (Exception e ){
//                    e.printStackTrace();
//                }
//                for(ClientWorker worker:arr.values())
//                {
//                    worker.shutdown();
//                }
//            }
//        }
        while(!stop){
            try{
                Thread.sleep(5000);
            }catch (Exception e ){
                e.printStackTrace();
            }
        }
        System.out.println("current seek time："+DateUtil.toChar(curTimestamp));
        monitorTimer.cancel();
        //删除consumer group;
        if(deleteConsumerGroup){
            try{
                StaticCredentialProvider provider = StaticCredentialProvider.create(Credential.builder()
                        .accessKeyId(sAccessKeyId)
                        .accessKeySecret(sAccessKey)
                        //.securityToken("<your-token>") // use STS token
                        .build());

                // Configure the Client
                AsyncClient client = AsyncClient.builder()
                        .region(region) // Region ID
                        //.httpClient(httpClient) // Use the configured HttpClient, otherwise use the default HttpClient (Apache HttpClient)
                        .credentialsProvider(provider)
                        //.serviceConfiguration(Configuration.create()) // Service-level configuration
                        // Client-level configuration rewrite, can set Endpoint, Http request parameters, etc.
                        .overrideConfiguration(
                                ClientOverrideConfiguration.create()
                                        .setEndpointOverride(sEndpoint)
                                //.setConnectTimeout(Duration.ofSeconds(30))
                        )
                        .build();
                // Parameter settings for API request
                DeleteConsumerGroupRequest deleteConsumerGroupRequest = DeleteConsumerGroupRequest.builder()
                        .project(sProject)
                        .logstore(sLogstore)
                        .consumerGroup(sConsumerGroup)
                        // Request-level configuration rewrite, can set Http request parameters, etc.
                        // .requestConfiguration(RequestConfiguration.create().setHttpHeaders(new HttpHeaders()))
                        .build();

                // Asynchronously get the return value of the API request
                CompletableFuture<com.aliyun.sdk.service.sls20201230.models.DeleteConsumerGroupResponse> response = client.deleteConsumerGroup(deleteConsumerGroupRequest);
                // Synchronously get the return value of the API request
                DeleteConsumerGroupResponse resp = response.get();
                System.out.println(new Gson().toJson(resp));
                client.close();
            }catch (Exception e){
                System.out.println("delete consumer group failed!");
                e.printStackTrace();
                System.exit(1);
            }

        }


    }

    public static void initArgs(String[] args){
        CommandLineParser parser = new GnuParser();
        Options options = new Options();
        options.addOption(null,"endpoint",true,"sls endpoint");
        options.addOption(null,"project",true,"sls project");
        options.addOption(null,"store",true,"sls store");
        options.addOption(null,"accesskeyid",true,"accessKeyId");
        options.addOption(null,"accesskey",true,"accessKey");
        options.addOption(null,"from",true,"from date,format '2001-01-01 00:00:00'");
        options.addOption(null,"to",true,"end date,format '2001-01-01 00:00:00'");
        options.addOption(null,"out",true,"output file path");
        options.addOption(null,"threads",true,"dump thread count");
        options.addOption(null,"sort-by-date",false,"sort by sql time,if set ,will print timestamp ahead");
        options.addOption(null,"region",true,"region:cn-hangzhou、cn-shanghai");
        options.addOption(null,"log-type",true,"rds-mysql、drds、default");
        options.addOption(null,"delete-consumer-group",false,"delete consumer group after download");
        options.addOption(null,"filter",true,"only download log when matched：--filter=\"instance_id=sdfwer234sdfsf\"");

        try{
            CommandLine commandLine = parser.parse( options, args );
            sEndpoint=commandLine.getOptionValue("endpoint");
            sProject=commandLine.getOptionValue("project");
            sLogstore=commandLine.getOptionValue("store");
            sAccessKey=commandLine.getOptionValue("accesskey");
            sAccessKeyId=commandLine.getOptionValue("accesskeyid");
            String from =commandLine.getOptionValue("from");
            String end =commandLine.getOptionValue("to");
            startTime=DateUtil.toDate(from,"yyyy-MM-dd HH:mm:ss").getTime()/1000;
            //endTime=DateUtil.toDate(end,"yyyy-MM-dd HH:mm:ss").getTime()/1000;
            outputFile=commandLine.getOptionValue("out");
            if(commandLine.hasOption("sort-by-date")){
                sortByDate=true;
            }
            if(commandLine.hasOption("delete-consumer-group")){
                deleteConsumerGroup=true;
            }
            if(commandLine.hasOption("region")){
                region=commandLine.getOptionValue("region");
            }
            if(commandLine.hasOption("log-type")){
                logType=commandLine.getOptionValue("log-type");
            }
            if(commandLine.hasOption("filter")){
                String filter=commandLine.getOptionValue("filter");
                String[] filterItems=filter.split(",");
                filterMap =new HashMap<>();
                for(String item:filterItems){
                    String[] filterEntry=item.split("=");
                    filterMap.put(filterEntry[0],filterEntry[1]);
                }
            }

            threads=Integer.valueOf(commandLine.getOptionValue("threads"));
        }catch (Exception e){
            e.printStackTrace();
            System.exit(1);
        }
    }
}
