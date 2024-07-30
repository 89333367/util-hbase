# kafka工具类

> 适用于 hbase 1.2.12 +
> 
> 适用于 jdk8+

> 引入依赖
```xml
<dependency>
    <groupId>sunyu.util</groupId>
    <artifactId>util-hbase</artifactId>
    <version>hbase-shaded-client-1.2.12_v1.0</version>
</dependency>
```

### 更多接口调用请看HbaseUtil.java源码
```java
public class TestHbaseUtil {
    Log log = LogFactory.get();
    HbaseUtil hbaseUtil = HbaseUtil.builder().setHbaseZookeeperQuorum("cdh0:2181,cdh1:2181,cdh2:2181").setZookeeperZnodeParent("/hbase").build();

    @Test
    void t001() {
        List<Map<String, String>> l = hbaseUtil.select("select * from farm_can#can where startRowKey='zzlic272318_20200524155905' and stopRowKey='zzlic272318_20200524160930'");
        for (Map<String, String> m : l) {
            log.info("{}", m);
        }
    }

    @Test
    void t002() {
        String sql = "select * from farm_can#can where startRowKey='zzlic272318_20200524155905' and stopRowKey='zzlic272318_20200524160930'";
        hbaseUtil.select(sql, null, row -> {
            log.info("{}", row);
        });
    }
}
```