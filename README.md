# kafka工具类

## 描述

* 可以用于建表/删表/修改表/禁用表/启用表/查询数据/统计行数/插入数据/更新数据/删除数据
* 特殊变量：rowKey/startRowKey/stopRowKey
* 注意：此工具类只适用于存储hbase都是字符串类型的数据查询

## 环境

* 适用于 hbase 1.2.12 及以上版本
* 适用于 jdk8 x64 及以上版本

## 引入依赖

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


    @Test
    void t001() {
        //工具类中全局只需要build一次
        HbaseUtil hbaseUtil = HbaseUtil.builder().hbaseZookeeperQuorum("cdh0:2181,cdh1:2181,cdh2:2181").zookeeperZnodeParent("/hbase").build();
        List<Map<String, String>> l = hbaseUtil.select("select * from farm_can#can where startRowKey='zzlic272318_20200524155905' and stopRowKey='zzlic272318_20200524160930'");
        for (Map<String, String> m : l) {
            log.info("{}", m);
        }
    }

    @Test
    void t002() {
        //工具类中全局只需要build一次
        HbaseUtil hbaseUtil = HbaseUtil.builder().hbaseZookeeperQuorum("cdh0:2181,cdh1:2181,cdh2:2181").zookeeperZnodeParent("/hbase").build();
        String sql = "select * from farm_can#can where startRowKey='zzlic272318_20200524155905' and stopRowKey='zzlic272318_20200524160930'";
        hbaseUtil.select(sql, null, row -> {
            log.info("{}", row);
        });
    }


    @Test
    void t003() {
        //工具类中全局只需要build一次
        HbaseUtil hbaseUtil = HbaseUtil.builder().hbaseZookeeperQuorum("cdh0:2181,cdh1:2181,cdh2:2181").zookeeperZnodeParent("/hbase").build();
        String sql = "select count(*) from farm_can#can where startRowKey='zzlic272318_20200524155905' and stopRowKey='zzlic272318_20200524160930'";
        log.info("{}", hbaseUtil.count(sql));
    }

    @Test
    void t004() {
        //工具类中全局只需要build一次
        HbaseUtil hbaseUtil = HbaseUtil.builder().hbaseZookeeperQuorum("cdh0:2181,cdh1:2181,cdh2:2181").zookeeperZnodeParent("/hbase").build();
        String sql = "select * from farm_can#can where startRowKey='zzlic272318_20200524155905' and stopRowKey='zzlic272318_20200524160930'";
        hbaseUtil.select(sql, null, row -> {
            log.info("{}", row);
        });
    }

    @Test
    void t005() {
        //工具类中全局只需要build一次
        HbaseUtil hbaseUtil = HbaseUtil.builder().hbaseZookeeperQuorum("cdh0:2181,cdh1:2181,cdh2:2181").zookeeperZnodeParent("/hbase").build();
        String sql = "select * from farm_can#can where startRowKey='zzlic272318_20200524155905' and stopRowKey='zzlic272318_20200524160930'";
        hbaseUtil.select(sql, null, row -> {
            log.info("{}", row);
        }, true);
    }

    @Test
    void t006() {
        //工具类中全局只需要build一次
        HbaseUtil hbaseUtil = HbaseUtil.builder().hbaseZookeeperQuorum("cdh0:2181,cdh1:2181,cdh2:2181").zookeeperZnodeParent("/hbase").build();
        AtomicInteger i = new AtomicInteger();
        String sql = "select * from farm_can#can where startRowKey='zzlic272318_20200524155905' and stopRowKey='zzlic272318_20200524160930'";
        hbaseUtil.select(sql, null, row -> {
            if (i.get() >= 10) {
                throw new Exception("只要10条，后面数据不要了");
            }
            log.info("{}", row);
            i.incrementAndGet();
        });
    }

    @Test
    void t007() {
        //工具类中全局只需要build一次
        HbaseUtil hbaseUtil = HbaseUtil.builder().hbaseZookeeperQuorum("cdh0:2181,cdh1:2181,cdh2:2181").zookeeperZnodeParent("/hbase").build();
        //程序关闭前，不再使用工具类了，调用close回收资源
        hbaseUtil.close();
    }

    @Test
    void t008() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "cdh0:2181,cdh1:2181,cdh2:2181");
        configuration.set("zookeeper.znode.parent", "/hbase");
        //工具类中全局只需要build一次
        HbaseUtil hbaseUtil = HbaseUtil.builder().build(configuration);

        String sql = "select count(*) from farm_can#can where startRowKey='zzlic272318_20200524155905' and stopRowKey='zzlic272318_20200524160930'";
        log.info("{}", hbaseUtil.count(sql));

        hbaseUtil.close();
    }
}
```