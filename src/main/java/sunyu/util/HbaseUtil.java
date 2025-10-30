package sunyu.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.util.JdbcConstants;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.exceptions.ExceptionUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ReUtil;
import cn.hutool.core.util.ReflectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import cn.hutool.system.SystemUtil;

/**
 * kafka生产者工具类
 *
 * <pre>
 * 可以用于建表/删表/修改表/禁用表/启用表/查询数据/统计行数/插入数据/更新数据/删除数据
 * 特殊变量：rowKey/startRowKey/stopRowKey
 * 注意：此工具类只适用于存储hbase都是字符串类型的数据查询
 * </pre>
 *
 * @author 孙宇
 */
public class HbaseUtil implements AutoCloseable {
    private final Log log = LogFactory.get();
    private final Config config;

    public static final String FIRST_VISIBLE_ASCII = "!";// ascii 第一个可见字符
    public static final String LAST_VISIBLE_ASCII = "~";// ascii 最后一个可见字符
    public static final String ROW_KEY_NAME = "rowKey";// 默认返回rowKey的名称
    public static final String START_ROW_KEY_NAME = "startRowKey";
    public static final String STOP_ROW_KEY_NAME = "stopRowKey";
    public static final String COPROCESSOR = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";

    public static Builder builder() {
        return new Builder();
    }

    private HbaseUtil(Config config) {
        log.info("[构建{}] 开始", this.getClass().getSimpleName());

        // 避免没有环境变量时报错
        if (SystemUtil.getOsInfo().isWindows()) {
            log.info("windows设置环境变量开始");
            try {
                if (StrUtil.isBlank(System.getProperty("hadoop.home.dir"))) {
                    File workaround = new File(".");
                    new File(".".concat(File.separator).concat("bin")).mkdirs();
                    new File(".".concat(File.separator).concat("bin").concat(File.separator).concat("winutils.exe"))
                            .createNewFile();
                    System.setProperty("hadoop.home.dir", workaround.getAbsolutePath());
                }
                log.info("windows设置环境变量结束");
            } catch (Exception e) {
                log.warn("{}", e.getMessage());
            }
        }

        if (config.configuration.get("zookeeper.znode.parent") == null) {
            config.configuration.set("zookeeper.znode.parent", "/hbase");
        }
        if (config.configuration.get("hbase.rpc.timeout") == null) {
            config.configuration.set("hbase.rpc.timeout", "" + 60000 * 10);
        }
        if (config.configuration.get("hbase.rpc.shortoperation.timeout") == null) {
            config.configuration.set("hbase.rpc.shortoperation.timeout", "" + 10000 * 10);
        }
        if (config.configuration.get("hbase.client.scanner.timeout.period") == null) {
            config.configuration.set("hbase.client.scanner.timeout.period", "" + 60000 * 10);
        }
        if (config.configuration.get("hbase.client.operation.timeout") == null) {
            config.configuration.set("hbase.client.operation.timeout", "" + config.timeout * 1000);
        }

        try {
            log.info("创建 hbase 链接开始");
            config.connection = ConnectionFactory.createConnection(config.configuration,
                    ThreadUtil.newExecutor(config.threadSize));
            log.info("创建 hbase 链接成功");
        } catch (Exception e) {
            log.error("创建 hbase 链接失败 {}", ExceptionUtil.stacktraceToString(e));
            throw new RuntimeException("创建 hbase 链接失败");
        }

        log.info("创建统计协处理器开始");
        config.aggregationClient = new AggregationClient(config.configuration);
        log.info("创建统计协处理器完毕");

        // 为了避免有些jar版本太低，导致setCaching方法不存在，这里先判断一下
        if (ReflectUtil.getMethod(Scan.class, "setCaching") != null) {
            log.info("标记Scan支持setCaching方法");
            config.canSetCaching = true;
        }
        log.info("[构建{}] 结束", this.getClass().getSimpleName());

        this.config = config;
    }

    private static class Config {
        private Connection connection;// hbase链接
        private AggregationClient aggregationClient;
        private final Configuration configuration = HBaseConfiguration.create();
        private volatile boolean canSetCaching = false;// 表示是否能设置caching
        private int threadSize = 1;
        private int timeout = 60;
        private final RegexUtil regexUtil = RegexUtil.builder().build();
    }

    public static class Builder {
        private final Config config = new Config();

        public HbaseUtil build() {
            return new HbaseUtil(config);
        }

        /**
         * 设置线程数量
         *
         * @param threadSize
         */
        public Builder threadSize(int threadSize) {
            config.threadSize = threadSize;
            return this;
        }

        /**
         * 设置超时时间(秒)
         *
         * @param timeout
         *
         * @return
         */
        public Builder timeout(int timeout) {
            config.timeout = timeout;
            return this;
        }

        /**
         * 设置zookeeper地址
         *
         * @param hbaseZookeeperQuorum zookeeper地址
         *
         * @return
         */
        public Builder hbaseZookeeperQuorum(String hbaseZookeeperQuorum) {
            config.configuration.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum);
            return this;
        }

        /**
         * 设置znode路径
         *
         * @param zookeeperZnodeParent znode路径
         *
         * @return
         */
        public Builder zookeeperZnodeParent(String zookeeperZnodeParent) {
            config.configuration.set("zookeeper.znode.parent", zookeeperZnodeParent);
            return this;
        }
    }

    /**
     * 回收资源
     */
    @Override
    public void close() {
        log.info("[销毁{}] 开始", this.getClass().getSimpleName());
        try {
            log.info("关闭 aggregation 开始");
            config.aggregationClient.close();
            log.info("关闭 aggregation 成功");
        } catch (Exception e) {
            log.warn("关闭 aggregation 失败 {}", ExceptionUtil.stacktraceToString(e));
        }
        try {
            log.info("关闭 Hbase 链接开始");
            config.connection.close();
            log.info("关闭 Hbase 链接成功");
        } catch (Exception e) {
            log.warn("关闭 Hbase 链接失败 {}", ExceptionUtil.stacktraceToString(e));
        }
        log.info("[销毁{}] 结束", this.getClass().getSimpleName());
    }

    /**
     * 把字符串最后一个字符的ascii向前挪一位
     *
     * @param str 原字符串
     *
     * @return 新字符串
     */
    public String lastCharAsciiSubOne(String str) {
        if (StrUtil.isBlank(str)) {
            return "";
        }
        char[] ca = str.toCharArray();
        ca[ca.length - 1] = (char) (ca[ca.length - 1] - 1);
        return new String(ca);
    }

    /**
     * 把字符串最后一个字符的ascii向后挪一位
     *
     * @param str 原字符串
     *
     * @return 新字符串
     */
    public String lastCharAsciiAddOne(String str) {
        if (StrUtil.isBlank(str)) {
            return "";
        }
        char[] ca = str.toCharArray();
        ca[ca.length - 1] = (char) (ca[ca.length - 1] + 1);
        return new String(ca);
    }

    /**
     * 删除表
     *
     * @param tableName *表名
     */
    public boolean deleteTable(String tableName) {
        try (Admin admin = config.connection.getAdmin();) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            log.error("删除表出现异常 {}", ExceptionUtil.stacktraceToString(e));
            return false;
        }
        return true;
    }

    /**
     * 判断表是否存在
     *
     * @param tableName *表名
     *
     * @return 表是否存在
     */
    public boolean existsTable(String tableName) {
        boolean exists = false;
        try (Admin admin = config.connection.getAdmin();) {
            exists = admin.tableExists(TableName.valueOf(tableName));
        } catch (IOException e) {
            log.error("判断表是否存在发生异常 {}", ExceptionUtil.stacktraceToString(e));
        }
        return exists;
    }

    /**
     * 创建表
     *
     * @param tableName     *表名
     * @param familyName    *列出名称
     * @param timeToLive    列簇超时时间，单位秒
     * @param splitKeyBytes 预分区参数
     */
    public boolean createTable(String tableName, String familyName, Integer timeToLive, byte[][] splitKeyBytes) {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        try {
            hTableDescriptor.addCoprocessor(COPROCESSOR);// 设置统计协处理器
        } catch (IOException e) {
            log.error("配置协处理器发生异常 {}", ExceptionUtil.stacktraceToString(e));
            return false;
        }
        hTableDescriptor.setCompactionEnabled(true);
        HColumnDescriptor family = new HColumnDescriptor(familyName);
        family.setMaxVersions(1);// 设置数据保存的最大版本数
        family.setCompressionType(Compression.Algorithm.SNAPPY);// 设置压缩
        if (timeToLive != null) {
            family.setTimeToLive(timeToLive);
        }
        hTableDescriptor.addFamily(family);
        try (Admin admin = config.connection.getAdmin();) {
            if (splitKeyBytes != null) {
                admin.createTable(hTableDescriptor, splitKeyBytes);
            } else {
                admin.createTable(hTableDescriptor);
            }
        } catch (IOException e) {
            log.error("创建表发生异常 {}", ExceptionUtil.stacktraceToString(e));
            return false;
        }
        return true;
    }

    /**
     * 创建表
     *
     * @param tableName  *表名
     * @param familyName *列簇名
     * @param timeToLive *列簇超时时间（秒）
     */
    public boolean createTable(String tableName, String familyName, Integer timeToLive) {
        return createTable(tableName, familyName, timeToLive, null);
    }

    /**
     * 创建表
     *
     * @param tableName  *表名
     * @param familyName *列簇名称
     */
    public boolean createTable(String tableName, String familyName) {
        return createTable(tableName, familyName, null, null);
    }

    /**
     * 禁用表
     *
     * @param tableName *表名
     */
    public boolean disableTable(String tableName) {
        try (Admin admin = config.connection.getAdmin();) {
            admin.disableTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            log.error("禁用表发生异常 {}", ExceptionUtil.stacktraceToString(e));
            return false;
        }
        return true;
    }

    /**
     * 启用表
     *
     * @param tableName *表名
     */
    public boolean enableTable(String tableName) {
        try (Admin admin = config.connection.getAdmin();) {
            admin.enableTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            log.error("启用表发生异常 {}", ExceptionUtil.stacktraceToString(e));
            return false;
        }
        return true;
    }

    /**
     * 修改表
     *
     * @param tableName *表名
     */
    public boolean modifyTable(String tableName) {
        return modifyTable(tableName, null);
    }

    /**
     * 修改表
     *
     * @param tableName  *表名
     * @param timeToLive *列簇超时时间，单位秒
     */
    public boolean modifyTable(String tableName, Integer timeToLive) {
        try (Admin admin = config.connection.getAdmin();
                Table table = config.connection.getTable(TableName.valueOf(tableName));) {
            disableTable(tableName);// 禁用表
            HTableDescriptor hTableDescriptor = table.getTableDescriptor();// 获得表描述
            if (!hTableDescriptor.getCoprocessors().contains(COPROCESSOR)) {
                hTableDescriptor.addCoprocessor(COPROCESSOR);// 设置统计协处理器
            }
            hTableDescriptor.setCompactionEnabled(true);
            hTableDescriptor.getFamilies().forEach(hColumnDescriptor -> {
                hColumnDescriptor.setMaxVersions(1);// 设置数据保存的最大版本数
                hColumnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);// 设置压缩
                if (timeToLive != null) {
                    hColumnDescriptor.setTimeToLive(timeToLive);
                } else {
                    hColumnDescriptor.setTimeToLive(Integer.MAX_VALUE);
                }
            });
            admin.modifyTable(TableName.valueOf(tableName), hTableDescriptor);// 修改表
        } catch (IOException e) {
            log.error("修改表发生异常 {}", ExceptionUtil.stacktraceToString(e));
            return false;
        } finally {
            enableTable(tableName);
        }
        return true;
    }

    /**
     * 删除一条记录
     *
     * @param tableName *表名
     * @param rowKey    *行键
     */
    public boolean delete(String tableName, String rowKey) {
        try (Table table = config.connection.getTable(TableName.valueOf(tableName));) {
            table.delete(new Delete(Bytes.toBytes(rowKey)));
        } catch (IOException e) {
            log.error("删除一条记录发生异常 {}", ExceptionUtil.stacktraceToString(e));
            return false;
        }
        return true;
    }

    /**
     * 删除一批记录
     *
     * @param tableName  *表名
     * @param rowKeyList *行键集合
     */
    public boolean delete(String tableName, List<String> rowKeyList) {
        List<Delete> deleteList = new ArrayList<>();
        for (String rowKey : rowKeyList) {
            deleteList.add(new Delete(Bytes.toBytes(rowKey)));
        }
        try (Table table = config.connection.getTable(TableName.valueOf(tableName));) {
            table.delete(deleteList);
        } catch (IOException e) {
            log.error("删除一批记录发生异常 {}", ExceptionUtil.stacktraceToString(e));
            return false;
        }
        return true;
    }

    /**
     * 根据rowKey删除指定列
     *
     * @param tableName  表名
     * @param familyName 列簇名
     * @param rowKey     rowKey
     * @param columns    删除列集合
     *
     * @return
     */
    public boolean deleteColumns(String tableName, String familyName, String rowKey, List<String> columns) {
        Delete del = new Delete(Bytes.toBytes(rowKey));
        columns.forEach(column -> {
            del.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column));
        });

        try (Table table = config.connection.getTable(TableName.valueOf(tableName));) {
            table.delete(del);
        } catch (IOException e) {
            log.error("删除指定列发生异常 {}", ExceptionUtil.stacktraceToString(e));
            return false;
        }
        return true;
    }

    /**
     * 存入一条记录
     *
     * @param tableName  *表名
     * @param familyName *列簇名
     * @param rowKey     *rowKey
     * @param data       *列值信息
     */
    public boolean put(String tableName, String familyName, String rowKey, Map<String, String> data) {
        Map<String, Map<String, String>> datas = new HashMap<>();
        datas.put(rowKey, data);
        return put(tableName, familyName, datas);
    }

    /**
     * 存入多条记录
     *
     * @param tableName  *表名
     * @param familyName *列簇名
     * @param datas      *rowKey以及列信息；key：rowKey，value(map)：k:列名,v:列值
     */
    public boolean put(String tableName, String familyName, Map<String, Map<String, String>> datas) {
        try (Table table = config.connection.getTable(TableName.valueOf(tableName));) {
            byte[] familyNameByte = Bytes.toBytes(familyName);
            List<Put> puts = new ArrayList<>();
            datas.forEach((rowKey, columnInfo) -> {
                Put put = new Put(Bytes.toBytes(rowKey));
                for (Map.Entry<String, String> infoEntry : columnInfo.entrySet()) {
                    put.addColumn(familyNameByte, Bytes.toBytes(infoEntry.getKey()),
                            Bytes.toBytes(infoEntry.getValue()));
                }
                puts.add(put);
            });
            table.put(puts);
        } catch (IOException e) {
            log.error("存入多条记录发生异常 {}", ExceptionUtil.stacktraceToString(e));
            return false;
        }
        return true;
    }

    /**
     * 统计行数
     *
     * <pre>
     * select count(*) from can_ne#can where startRowKey = '00004baa3388ab01e3d153347e7fc163_20190101000000' and stopRowKey = '00004baa3388ab01e3d153347e7fc163_20191231000000' and 2601 = 0
     * 也可以加更多条件，使用方式与select方法类似
     * 注意：startRowKey的值必须小于stopRowKey的值
     * </pre>
     *
     * @param ql *查询语句
     *
     * @return 匹配的行数
     */
    public long count(String ql) {
        return count(ql, null);
    }

    /**
     * 统计行数
     *
     * <pre>
     * select count(*) from can_ne#can where startRowKey = '00004baa3388ab01e3d153347e7fc163_20190101000000' and stopRowKey = '00004baa3388ab01e3d153347e7fc163_20191231000000' and 2601 = 0
     * 也可以加更多条件，使用方式与select方法类似
     * 注意：startRowKey的值必须小于stopRowKey的值
     * </pre>
     *
     * @param ql                *查询语句
     * @param columnsCanMissing *默认where里的列必须存在，如果传递这个参数，则列可以不存在，英文半角逗号分隔
     *
     * @return 匹配的行数
     */
    public long count(String ql, String columnsCanMissing) {
        // 去掉ql中的order by和limit信息，count不需要这两个信息
        String orderBy = ReUtil.get("[ ]order[ ]+by[ ]+", ql, 0);
        if (StrUtil.isNotBlank(orderBy)) {
            ql = ql.substring(0, ql.indexOf(orderBy));
        }
        String limitN = ReUtil.get("[ ]limit[ ]+[0-9]+", ql, 0);
        if (StrUtil.isNotBlank(limitN)) {
            ql = ql.substring(0, ql.indexOf(limitN));
        }

        log.debug("QL：{}", ql);
        List<String> columnCanMissingList = null;
        if (StrUtil.isNotBlank(columnsCanMissing)) {
            log.debug("columnsCanMissing：{}", columnsCanMissing);
            columnCanMissingList = Arrays.asList(columnsCanMissing.split(","));
        }
        Scan scan = new Scan();
        String tableName = null;
        byte[] familyNameBytes;
        // 解析
        List<SQLStatement> statements = SQLUtils.parseStatements(ql, JdbcConstants.HBASE);
        // 只考虑一条语句
        SQLStatement statement = statements.get(0);
        // 只考虑查询语句
        SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) statement;
        SQLSelectQuery sqlSelectQuery = sqlSelectStatement.getSelect().getQuery();
        // 非union的查询语句
        if (sqlSelectQuery instanceof SQLSelectQueryBlock) {
            SQLSelectQueryBlock sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlSelectQuery;
            // 获取表
            SQLTableSource from = sqlSelectQueryBlock.getFrom();
            String[] tableSource = from.toString().split("#");
            tableName = tableSource[0];
            familyNameBytes = Bytes.toBytes(tableSource[1]);
            scan.addFamily(familyNameBytes);
            log.trace("表名#列簇：{}#{}", tableName, tableSource[1]);
            // 获取where条件
            SQLExpr where = sqlSelectQueryBlock.getWhere();
            if (where instanceof SQLBinaryOpExpr) {// 二元表达式
                log.trace("查询条件：");
                scan.setFilter(new FilterList());
                parseQl(familyNameBytes, scan, true, where, null, columnCanMissingList);
            }
            FilterList filterList = (FilterList) scan.getFilter();
            if (filterList != null && CollUtil.isEmpty(filterList.getFilters())) {
                scan.setFilter(null);
            }

            String startRow = Bytes.toString(scan.getStartRow());
            String stopRow = Bytes.toString(scan.getStopRow());
            if (StrUtil.isNotBlank(startRow) && StrUtil.isNotBlank(stopRow)) {
                if (startRow.compareTo(stopRow) > 0) {
                    throw new RuntimeException(StrUtil.format("startRowKey[{}]不能大于stopRowKey[{}]", startRow, stopRow));
                }
            }

            log.trace("{}", scan);

            try {
                return config.aggregationClient.rowCount(TableName.valueOf(tableName), new LongColumnInterpreter(),
                        scan);
            } catch (Throwable throwable) {
                log.error("统计数量发生异常 {}", ExceptionUtil.stacktraceToString(throwable));
            }
        }
        return 0;
    }

    /**
     * 查询
     *
     * <pre>
     * 查询必须填写表名和列簇名称(表名#列簇)，必须添加 limit ，否则查询所有数据太慢
     * 查询所有列信息：select * from can_ne#can limit 10
     * 查询某些列信息：select 3014,2205,did from can_ne#can limit 10
     * 如果知道起始位置：select * from can_ne#can where startRowKey = '00004baa3388ab01e3d153347e7fc163_20190101000000' limit 10
     * 如果知道结束位置：select * from can_ne#can where stopRowKey = '00004baa3388ab01e3d153347e7fc163_20191231000000' limit 10
     * 如果知道起始与结束位置：select * from can_ne#can where startRowKey = '00004baa3388ab01e3d153347e7fc163_20190101000000' and stopRowKey = '00004baa3388ab01e3d153347e7fc163_20191231000000' limit 10
     * 只查询rowKey：select rowKey from can_ne#can where startRowKey = '00004baa3388ab01e3d153347e7fc163_20190101000000' and stopRowKey = '00004baa3388ab01e3d153347e7fc163_20191231000000' limit 10
     * 如果知道rowKey，只查一条：select * from can_ne#can where startRowKey = '00004baa3388ab01e3d153347e7fc163_20191125065602' and stopRowKey='00004baa3388ab01e3d153347e7fc163_20191125065602'
     * 如果不写order by，那么默认升序
     * 降序查询需要注意，startRowKey的值必须比stopRowKey的值大，并且需要写 order by rowKey desc，例如：
     * select 2205,did,TIME,3014 from can_ne#can  where startRowKey = '00004baa3388ab01e3d153347e7fc163_20191231000000'  and stopRowKey = '00004baa3388ab01e3d153347e7fc163_20190101000000' order by rowKey desc limit 10
     * 查询列不为空的写法：select * from gateway#log where 2909 != '' limit 10
     * 正则用法，以REG#开头，后面是正则表达式：select * from command#command where rowKey = 'REG#test123456789_.*_3' order by rowKey desc limit 1
     * 查询中，可以使用 and , or , like , not like , = , != , > , >= , < , <=
     * 具体使用方式，请查看TestQlParser
     * 注意：查询中，包含startRowKey，也包含stopRowKey；如果列明有特殊字符，需要使用 ` 符号包裹
     * </pre>
     *
     * @param ql *查询语句
     *
     * @return 结果集
     */
    public List<Map<String, String>> select(String ql) {
        return select(ql, null);
    }

    /**
     * 查询
     *
     * <pre>
     * 查询必须填写表名和列簇名称(表名#列簇)，必须添加 limit ，否则查询所有数据太慢
     * 查询所有列信息：select * from can_ne#can limit 10
     * 查询某些列信息：select 3014,2205,did from can_ne#can limit 10
     * 如果知道起始位置：select * from can_ne#can where startRowKey = '00004baa3388ab01e3d153347e7fc163_20190101000000' limit 10
     * 如果知道结束位置：select * from can_ne#can where stopRowKey = '00004baa3388ab01e3d153347e7fc163_20191231000000' limit 10
     * 如果知道起始与结束位置：select * from can_ne#can where startRowKey = '00004baa3388ab01e3d153347e7fc163_20190101000000' and stopRowKey = '00004baa3388ab01e3d153347e7fc163_20191231000000' limit 10
     * 只查询rowKey：select rowKey from can_ne#can where startRowKey = '00004baa3388ab01e3d153347e7fc163_20190101000000' and stopRowKey = '00004baa3388ab01e3d153347e7fc163_20191231000000' limit 10
     * 如果知道rowKey，只查一条：select * from can_ne#can where startRowKey = '00004baa3388ab01e3d153347e7fc163_20191125065602' and stopRowKey='00004baa3388ab01e3d153347e7fc163_20191125065602'
     * 如果不写order by，那么默认升序
     * 降序查询需要注意，startRowKey的值必须比stopRowKey的值大，并且需要写 order by rowKey desc，例如：
     * select 2205,did,TIME,3014 from can_ne#can  where startRowKey = '00004baa3388ab01e3d153347e7fc163_20191231000000'  and stopRowKey = '00004baa3388ab01e3d153347e7fc163_20190101000000' order by rowKey desc limit 10
     * 查询列不为空的写法：select * from gateway#log where 2909 != '' limit 10
     * 正则用法，以REG#开头，后面是正则表达式：select * from command#command where rowKey = 'REG#test123456789_.*_3' order by rowKey desc limit 1
     * 查询中，可以使用 and , or , like , not like , = , != , > , >= , < , <=
     * 具体使用方式，请查看TestQlParser
     * 注意：查询中，包含startRowKey，也包含stopRowKey；如果列明有特殊字符，需要使用 ` 符号包裹
     * </pre>
     *
     * @param ql                *查询语句
     * @param columnsCanMissing *默认where里的列必须存在，如果传递这个参数，则列可以不存在，英文半角逗号分隔
     *
     * @return 结果集
     */
    public List<Map<String, String>> select(String ql, String columnsCanMissing) {
        List<Map<String, String>> datas = new ArrayList<>();
        select(ql, columnsCanMissing, row -> datas.add(row));
        log.debug("扫描到 {} 条记录", datas.size());
        return datas;
    }

    /**
     * 查询
     *
     * @param ql
     * @param columnsCanMissing
     * @param handler
     */
    public void select(String ql, String columnsCanMissing, java.util.function.Consumer<Map<String, String>> handler) {
        select(ql, columnsCanMissing, handler, null);
    }

    /**
     * 查询
     *
     * @param ql
     * @param columnsCanMissing
     * @param handler               根据scanner回调
     * @param returnColumnTimestamp 返回列的插入时间
     */
    public void select(String ql, String columnsCanMissing, java.util.function.Consumer<Map<String, String>> handler,
            Boolean returnColumnTimestamp) {
        log.debug("QL：{}", ql);
        List<String> columnCanMissingList = null;
        if (StrUtil.isNotBlank(columnsCanMissing)) {
            log.debug("columnsCanMissing：{}", columnsCanMissing);
            columnCanMissingList = Arrays.asList(columnsCanMissing.split(","));
        }
        Scan scan = new Scan();
        String tableName = null;
        byte[] familyNameBytes;
        int pageSize = Integer.MAX_VALUE;
        List<String> selectColumns = new ArrayList<>();
        // 解析
        List<SQLStatement> statements = SQLUtils.parseStatements(ql, JdbcConstants.HBASE);
        // 只考虑一条语句
        SQLStatement statement = statements.get(0);
        // 只考虑查询语句
        SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) statement;
        SQLSelectQuery sqlSelectQuery = sqlSelectStatement.getSelect().getQuery();
        // 非union的查询语句
        if (sqlSelectQuery instanceof SQLSelectQueryBlock) {
            SQLSelectQueryBlock sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlSelectQuery;
            // 获取表
            SQLTableSource from = sqlSelectQueryBlock.getFrom();
            String[] tableSource = from.toString().split("#");
            tableName = tableSource[0];
            familyNameBytes = Bytes.toBytes(tableSource[1]);
            scan.addFamily(familyNameBytes);
            log.trace("表名#列簇：{}#{}", tableName, tableSource[1]);
            // 获取字段列表
            List<SQLSelectItem> selectItems = sqlSelectQueryBlock.getSelectList();
            log.trace("需要查询字段：{}", selectItems);
            if (selectItems.size() == 1) {
                String selectColumn = StrUtil.strip(selectItems.get(0).toString(), "`");
                if (selectColumn.equals(ROW_KEY_NAME)) {
                    FilterList filterList = new FilterList();
                    filterList.addFilter(new KeyOnlyFilter());
                    scan.setFilter(filterList);
                } else if (!selectColumn.equals("*")) {
                    selectColumns.add(selectColumn);
                    scan.addColumn(familyNameBytes, Bytes.toBytes(selectColumn));
                }
            } else {
                for (SQLSelectItem selectItem : selectItems) {
                    String selectColumn = StrUtil.strip(selectItem.toString(), "`");
                    selectColumns.add(selectColumn);
                    scan.addColumn(familyNameBytes, Bytes.toBytes(selectColumn));
                }
            }
            // 获取排序
            SQLOrderBy orderBy = sqlSelectQueryBlock.getOrderBy();
            if (orderBy != null) {
                String orderByType = orderBy.getItems().get(0).getType().name();
                log.trace("rowKey排序：{}", orderByType);
                if (orderByType.equals("DESC")) {
                    scan.setReversed(true);
                } else {
                    scan.setReversed(false);
                }
            }
            // 获取分页
            SQLLimit limit = sqlSelectQueryBlock.getLimit();
            if (limit != null) {
                pageSize = Integer.parseInt(limit.getRowCount().toString());
                log.trace("查询 {} 条", pageSize);
                if (config.canSetCaching) {
                    if (pageSize > 1000) {
                        scan.setCaching(1000);
                    } else {
                        scan.setCaching(pageSize);
                    }
                }
            }
            // 获取where条件
            SQLExpr where = sqlSelectQueryBlock.getWhere();
            if (where instanceof SQLBinaryOpExpr) {// 二元表达式
                log.trace("查询条件：");
                FilterList filterList = (FilterList) scan.getFilter();
                if (filterList == null) {
                    scan.setFilter(new FilterList());
                }
                parseQl(familyNameBytes, scan, CollUtil.isEmpty(selectColumns), where, null, columnCanMissingList);
            }
        }
        FilterList filterList = (FilterList) scan.getFilter();
        if (filterList != null && CollUtil.isEmpty(filterList.getFilters())) {
            scan.setFilter(null);
        }

        String startRow = Bytes.toString(scan.getStartRow());
        String stopRow = Bytes.toString(scan.getStopRow());
        if (StrUtil.isNotBlank(startRow) && StrUtil.isNotBlank(stopRow)) {
            if (scan.isReversed()) {
                if (startRow.compareTo(stopRow) < 0) {
                    throw new RuntimeException(
                            StrUtil.format("逆序查询数据，startRowKey[{}]不能小于stopRowKey[{}]", startRow, stopRow));
                }
            } else {
                if (startRow.compareTo(stopRow) > 0) {
                    throw new RuntimeException(
                            StrUtil.format("正序查询数据，startRowKey[{}]不能大于stopRowKey[{}]", startRow, stopRow));
                }
            }
        }

        log.trace("{}", scan);

        try (Table table = config.connection.getTable(TableName.valueOf(tableName));) {
            int i = 0;
            for (Result result : table.getScanner(scan)) {
                Map<String, String> row = new HashMap<>();
                row.put(ROW_KEY_NAME, Bytes.toString(result.getRow()));
                for (Cell cell : result.rawCells()) {// 循环所有列
                    String value = Bytes.toString(CellUtil.cloneValue(cell));// 列值
                    long timestamp = cell.getTimestamp();
                    if (StrUtil.isNotBlank(value)) {
                        String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));// 列名
                        if (!columnName.equals(ROW_KEY_NAME)) {
                            if (CollUtil.isEmpty(selectColumns) || selectColumns.contains(columnName)) {
                                if (BooleanUtil.isTrue(returnColumnTimestamp)) {
                                    row.put(columnName,
                                            new DateTime(timestamp).toString(DatePattern.NORM_DATETIME_MS_FORMAT));
                                } else {
                                    row.put(columnName, value);
                                }
                            }
                        }
                    }
                }
                try {
                    handler.accept(row);
                } catch (Exception e) {
                    log.warn("数据回调处理发生异常 {}", ExceptionUtil.stacktraceToString(e));
                    break;
                }
                if (++i == pageSize) {
                    break;
                }
            }
        } catch (Exception e) {
            log.error("查询数据发生异常 {}", ExceptionUtil.stacktraceToString(e));
        }
    }

    /**
     * 解析QL
     *
     * @param familyNameBytes
     * @param scan
     * @param selectAllColumn
     * @param sqlExpr
     * @param filterList
     * @param columnCanMissingList
     */
    private void parseQl(byte[] familyNameBytes, Scan scan, boolean selectAllColumn, SQLExpr sqlExpr,
            FilterList filterList, List<String> columnCanMissingList) {
        FilterList fl = filterList;
        SQLBinaryOpExpr expr = (SQLBinaryOpExpr) sqlExpr;
        SQLExpr left = expr.getLeft();
        SQLBinaryOperator operator = expr.getOperator();
        SQLExpr right = expr.getRight();
        if (fl == null) {
            if (operator == SQLBinaryOperator.BooleanOr) {
                fl = new FilterList(FilterList.Operator.MUST_PASS_ONE);
                FilterList scanFilterList = (FilterList) scan.getFilter();
                scanFilterList.addFilter(fl);
            } else {
                fl = (FilterList) scan.getFilter();
            }
        }
        if (right instanceof SQLBinaryOpExpr) {
            SQLBinaryOperator rightOperator = ((SQLBinaryOpExpr) right).getOperator();
            if (rightOperator == SQLBinaryOperator.BooleanOr) {
                FilterList rightFl = new FilterList(FilterList.Operator.MUST_PASS_ONE);
                fl.addFilter(rightFl);
                fl = rightFl;
            }
            parseQl(familyNameBytes, scan, selectAllColumn, right, fl, columnCanMissingList);
        }
        if (left instanceof SQLBinaryOpExpr) {
            parseQl(familyNameBytes, scan, selectAllColumn, left, fl, columnCanMissingList);
        }
        if (left instanceof SQLIdentifierExpr || left instanceof SQLIntegerExpr) {
            addStartRowKeyAndStopRowKeyAndFilter(familyNameBytes, scan, selectAllColumn, left, operator, right, fl,
                    columnCanMissingList);
        }
    }

    /**
     * 给scan添加startRowKey与stopRowKey与filter
     *
     * @param familyNameBytes
     * @param scan
     * @param selectAllColumn
     * @param left
     * @param operator
     * @param right
     * @param filterIfMissingColumnList
     */
    private void addStartRowKeyAndStopRowKeyAndFilter(byte[] familyNameBytes, Scan scan, boolean selectAllColumn,
            SQLExpr left, SQLBinaryOperator operator, SQLExpr right, FilterList filterList,
            List<String> filterIfMissingColumnList) {
        String columnName = StrUtil.strip(left.toString(), "`");
        String columnValue = right.toString();
        String type = right.computeDataType().getName();
        if (type.contains("char")) {
            columnValue = StrUtil.strip(columnValue, "'");
        }
        if (columnName.equals(START_ROW_KEY_NAME)) {
            log.trace("添加 {}", START_ROW_KEY_NAME);
            scan.setStartRow(Bytes.toBytes(columnValue));
            // scan.withStartRow(Bytes.toBytes(columnValue), true);
        } else if (columnName.equals(STOP_ROW_KEY_NAME)) {
            log.trace("添加 {}", STOP_ROW_KEY_NAME);
            String fixStopRow = null;
            if (scan.isReversed()) {
                fixStopRow = lastCharAsciiSubOne(columnValue);
            } else {
                fixStopRow = lastCharAsciiAddOne(columnValue);
            }
            scan.setStopRow(Bytes.toBytes(fixStopRow));
            // scan.withStopRow(Bytes.toBytes(columnValue), true);
        } else {
            if (!selectAllColumn) {
                scan.addColumn(familyNameBytes, Bytes.toBytes(columnName));
            }
            log.trace("添加条件 {} {} {}", columnName, operator.getName(), columnValue);
            SingleColumnValueFilter singleColumnValueFilter = null;
            RowFilter rowFilter = null;
            if (operator == SQLBinaryOperator.Equality) {
                if (columnValue.startsWith("REG#")) {
                    String regex = columnValue.substring(columnValue.indexOf("#") + 1);
                    if (columnName.equals(ROW_KEY_NAME)) {
                        rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex));
                    } else {
                        singleColumnValueFilter = new SingleColumnValueFilter(familyNameBytes,
                                Bytes.toBytes(columnName), CompareFilter.CompareOp.EQUAL,
                                new RegexStringComparator(regex));
                    }
                } else {
                    if (NumberUtil.isNumber(columnValue)) {
                        singleColumnValueFilter = new SingleColumnValueFilter(familyNameBytes,
                                Bytes.toBytes(columnName), CompareFilter.CompareOp.EQUAL,
                                new RegexStringComparator(config.regexUtil.transformEqualNumber(columnValue)));
                    } else {
                        singleColumnValueFilter = new SingleColumnValueFilter(familyNameBytes,
                                Bytes.toBytes(columnName), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(columnValue));
                    }
                }
            } else if (operator == SQLBinaryOperator.NotEqual) {
                if (columnValue.startsWith("REG#")) {
                    String regex = columnValue.substring(columnValue.indexOf("#") + 1);
                    if (columnName.equals(ROW_KEY_NAME)) {
                        rowFilter = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, new RegexStringComparator(regex));
                    } else {
                        singleColumnValueFilter = new SingleColumnValueFilter(familyNameBytes,
                                Bytes.toBytes(columnName), CompareFilter.CompareOp.NOT_EQUAL,
                                new RegexStringComparator(regex));
                    }
                } else {
                    if (NumberUtil.isNumber(columnValue)) {
                        singleColumnValueFilter = new SingleColumnValueFilter(familyNameBytes,
                                Bytes.toBytes(columnName), CompareFilter.CompareOp.NOT_EQUAL,
                                new RegexStringComparator(config.regexUtil.transformEqualNumber(columnValue)));
                    } else {
                        singleColumnValueFilter = new SingleColumnValueFilter(familyNameBytes,
                                Bytes.toBytes(columnName), CompareFilter.CompareOp.NOT_EQUAL,
                                Bytes.toBytes(columnValue));
                    }
                }
            } else if (operator == SQLBinaryOperator.Like) {
                if (columnName.equals(ROW_KEY_NAME)) {
                    rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(columnValue));
                } else {
                    singleColumnValueFilter = new SingleColumnValueFilter(familyNameBytes, Bytes.toBytes(columnName),
                            CompareFilter.CompareOp.EQUAL, new SubstringComparator(columnValue));
                }
            } else if (operator == SQLBinaryOperator.NotLike) {
                if (columnName.equals(ROW_KEY_NAME)) {
                    rowFilter = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, new SubstringComparator(columnValue));
                } else {
                    singleColumnValueFilter = new SingleColumnValueFilter(familyNameBytes, Bytes.toBytes(columnName),
                            CompareFilter.CompareOp.NOT_EQUAL, new SubstringComparator(columnValue));
                }
            } else if (operator == SQLBinaryOperator.GreaterThan) {
                singleColumnValueFilter = new SingleColumnValueFilter(familyNameBytes, Bytes.toBytes(columnName),
                        CompareFilter.CompareOp.EQUAL,
                        new RegexStringComparator(config.regexUtil.transformGreaterNumber(columnValue)));
            } else if (operator == SQLBinaryOperator.GreaterThanOrEqual) {
                singleColumnValueFilter = new SingleColumnValueFilter(familyNameBytes, Bytes.toBytes(columnName),
                        CompareFilter.CompareOp.EQUAL,
                        new RegexStringComparator(config.regexUtil.transformGreaterOrEqualNumber(columnValue)));
            } else if (operator == SQLBinaryOperator.LessThan) {
                singleColumnValueFilter = new SingleColumnValueFilter(familyNameBytes, Bytes.toBytes(columnName),
                        CompareFilter.CompareOp.EQUAL,
                        new RegexStringComparator(config.regexUtil.transformLessNumber(columnValue)));
            } else if (operator == SQLBinaryOperator.LessThanOrEqual) {
                singleColumnValueFilter = new SingleColumnValueFilter(familyNameBytes, Bytes.toBytes(columnName),
                        CompareFilter.CompareOp.EQUAL,
                        new RegexStringComparator(config.regexUtil.transformLessOrEqualNumber(columnValue)));
            }
            if (singleColumnValueFilter != null) {
                if (CollUtil.isNotEmpty(filterIfMissingColumnList) && filterIfMissingColumnList.contains(columnName)) {
                    singleColumnValueFilter.setFilterIfMissing(false);
                } else {
                    singleColumnValueFilter.setFilterIfMissing(true);
                }
                filterList.addFilter(singleColumnValueFilter);
            }
            if (rowFilter != null) {
                filterList.addFilter(rowFilter);
            }
        }
        log.trace("{}", scan);
    }

}