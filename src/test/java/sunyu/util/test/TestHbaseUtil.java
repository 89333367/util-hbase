package sunyu.util.test;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.junit.jupiter.api.Test;
import sunyu.util.HbaseUtil;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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


    @Test
    void t003() {
        String sql = "select count(*) from farm_can#can where startRowKey='zzlic272318_20200524155905' and stopRowKey='zzlic272318_20200524160930'";
        log.info("{}", hbaseUtil.count(sql));
    }

    @Test
    void t004() {
        String sql = "select * from farm_can#can where startRowKey='zzlic272318_20200524155905' and stopRowKey='zzlic272318_20200524160930'";
        hbaseUtil.select(sql, null, row -> {
            log.info("{}", row);
        });
    }

    @Test
    void t005() {
        String sql = "select * from farm_can#can where startRowKey='zzlic272318_20200524155905' and stopRowKey='zzlic272318_20200524160930'";
        hbaseUtil.select(sql, null, row -> {
            log.info("{}", row);
        }, true);
    }

    @Test
    void t006() {
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
}
