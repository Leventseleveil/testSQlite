package start;

import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.TransactionalIndexedCollection;
import com.googlecode.cqengine.index.hash.HashIndex;
import com.googlecode.cqengine.index.navigable.NavigableIndex;
import com.googlecode.cqengine.index.suffix.SuffixTreeIndex;
import com.googlecode.cqengine.index.unique.UniqueIndex;
import com.googlecode.cqengine.persistence.onheap.OnHeapPersistence;
import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.query.QueryFactory;
import com.googlecode.cqengine.query.logical.And;
import com.googlecode.cqengine.query.parser.sql.SQLParser;
import com.googlecode.cqengine.query.simple.Equal;
import com.googlecode.cqengine.resultset.ResultSet;
import entity.BondData;
import entity.BondExtraData;
import entity.BondMixedData;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.ParseException;
import java.util.*;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.googlecode.cqengine.query.QueryFactory.*;

public class TestCqengine {

    private static final Logger logger = LoggerFactory.getLogger(TestCqengine.class);
    static Connection connection = null;

    private static String[] bondName = new String[]{"22附息国债04", "19四川债37", "22昆明交产PPN003", "22鲁商SCP015",
            "22民生银行CD512", "21建设银行二级05", "21工商银行二级02", "22进出05", "22北京农商银行CD027", "20鄂租01"};

    private static String[] contributorID = new String[]{"PATR", "CCTB", "TPSH", "CNEX", "TJXT"};

    private static String[] multiBidVolume = new String[]{"1000", "2000", "3000", "4000", "5000", "6000", "7000"};

    private static Float[] askVolume = new Float[]{1000.0f, 2000.0f, 3000.0f, 4000.0f, 5000.0f, 6000.0f, 7000.0f};

    private static String[] securityID = new String[]{"032000446", "220215", "2228017", "012282317",
            "102101148", "220215", "012282164", "102101148", "012282696", "012282297"};

    private static String[] displayListedMarket = new String[]{"IB", "SH", "SZ"};

    private static String[] marketDataTime = new String[]{"2022-12-06 11:51:20.280", "2022-12-07 11:51:28.999", "2022-12-08 11:51:42.979", "2022-12-09 11:52:26.696",
            "2022-12-10 11:52:26.696", "2022-12-11 11:52:26.696", "2022-12-13 11:52:26.696", "2022-12-14 11:52:26.696", "2022-12-15 11:52:26.696", "2022-12-16 11:52:26.696"};

    private static String[] testName = new String[]{"测试用名称1", "测试用名称2", "测试用名称3", "随机名称1", "随机名称2", "随机名称3"};

    private static String[] testIntro = new String[]{"测试用内容1", "测试用内容2", "测试用内容3", "随机内容1", "随机内容2", "随机内容3"};

    private static String[] testCreateTime = new String[]{"2022-12-06 11:51:20.280", "2022-12-07 11:51:28.999", "2022-12-08 11:51:42.979", "2022-12-09 11:52:26.696",
            "2022-12-10 11:52:26.696", "2022-12-11 11:52:26.696", "2022-12-13 11:52:26.696", "2022-12-14 11:52:26.696", "2022-12-15 11:52:26.696", "2022-12-16 11:52:26.696"};

    private static String[] testUpdateTime = new String[]{"2022-12-06 11:51:20.280", "2022-12-07 11:51:28.999", "2022-12-08 11:51:42.979", "2022-12-09 11:52:26.696",
            "2022-12-10 11:52:26.696", "2022-12-11 11:52:26.696", "2022-12-13 11:52:26.696", "2022-12-14 11:52:26.696", "2022-12-15 11:52:26.696", "2022-12-16 11:52:26.696"};


    private static IndexedCollection<BondData> list;

    /**
     * 关联表
     */
    private static IndexedCollection<BondExtraData> extraList;

    /**
     * 测试用例集合
     */
    private static List<BondData> testData;

    /**
     * 关联表测试用例集合
     */
    private static List<BondExtraData> testExtraData;

    /**
     * 开启排序开关
     */
    private static final boolean WITH_ORDER = true;

    /**
     * 关闭排序开关
     */
    private static final boolean WITHOUT_ORDER = false;

    /**
     * 开启事务开关
     */
    private static final boolean WITH_TRANSACTION = true;

    /**
     * 关闭事务开关
     */
    private static final boolean WITHOUT_TRANSACTION = false;

    static {
        try {
            // 加载SQLite的JDBC驱动程序
            Class.forName("org.sqlite.JDBC");
            // 创建连接
            connection = DriverManager.getConnection("jdbc:sqlite::memory:");
        } catch (Exception e) {

        }
    }


    public static void main(String[] args) throws Exception {

        // 测试写入性能
        // testInsert(1000000);

        // 测试查询性能
        //testSelect(100000, 100, WITH_ORDER);

        // 测试聚合查询性能
        //testAggregationQuery(100000, 100);

        // 测试关联查询性能
        //testJoinQuery(100000, 100000, 100);

        // 测试SQLite并发读写性能
        // testSQLiteConcurrentReadWrite(100000, 1000, 20, 50);

        //测试SQLite并发写入性能
        //请你帮我修改以下代码中的testSQLiteConcurrentWrite方法，测试两个线程并发写入会不会导致锁表的问题
        testSQLiteConcurrentWrite(100000,1000, 20);

        // 测试CQEngine并发读写性能
        //testCQEngineConcurrentReadWrite(100000, 1000, WITH_TRANSACTION, 20, 50);

    }



    /**
     * 测试插入性能
     * @param size 测试用例大小
     * @throws Exception
     */
    public static void testInsert(int size) throws Exception {
        // 创建测试用例
        generateTestData(size);

        // 创建SQLite库表
        createSQLiteTable();
        // 插入所有测试用例到SQLite
        testInsertToSQLite();
        // 清空SQLite
        clearSQLite();

        // 在SQLite表建立索引
        createIndexOnSQLite();
        // 插入所有测试用例到SQLite
        testInsertToSQLite();

        // 初始化CQEngine，设置集合为ConcurrentIndexedCollection
        renewCQEngineCollection(WITHOUT_TRANSACTION);
        // 在CQEngine建立索引
        createIndexOnCQEngine();
        // 插入所有测试用例到CQEngine
        testInsertToCQEngine();

        // 初始化CQEngine，设置集合为TransactionalIndexedCollection
        renewCQEngineCollection(WITH_TRANSACTION);
        // 在CQEngine建立索引
        createIndexOnCQEngine();
        // 插入所有测试用例到CQEngine
        testInsertToCQEngine();

    }

    /**
     * 测试查询性能
     * @param dataSize 测试用例大小
     * @param queryTimes 查询次数
     * @param orderFlag 排序开关
     */
    public static void testSelect(int dataSize, int queryTimes, boolean orderFlag) throws Exception {
        // 创建测试用例
        generateTestData(dataSize);

        // 计时器 1.SQLite（未建立索引） 2.SQLite（建立索引） 3.CQEngine（ConcurrentIndexedCollection） 4.CQEngine（TransactionalIndexedCollection）
        long[] totalCost = new long[4];
        long[] maxCost = new long[4];

        // 创建SQLite库表
        createSQLiteTable();
        // 插入所有测试用例到SQLite
        testInsertToSQLite();
        // 统计查询时间
        for (int i = 0; i < queryTimes; i++) {
            long start = System.currentTimeMillis();
            testSQLiteQuery(orderFlag);
            long cost = System.currentTimeMillis() - start;
            totalCost[0] += cost;
            maxCost[0] = Math.max(maxCost[0], cost);
        }
        // 清空SQLite
        clearSQLite();

        // 在SQLite创建索引
        createIndexOnSQLite();
        // 插入所有测试用例到SQLite
        testInsertToSQLite();
        // 统计查询时间
        for (int i = 0; i < queryTimes; i++) {
            long start = System.currentTimeMillis();
            testSQLiteQuery(orderFlag);
            long cost = System.currentTimeMillis() - start;
            totalCost[1] += cost;
            maxCost[1] = Math.max(maxCost[1], cost);
        }
        // 清空SQLite
        clearSQLite();

        // 初始化CQEngine，设置集合为ConcurrentIndexedCollection
        renewCQEngineCollection(WITHOUT_TRANSACTION);
        // 在CQEngine建立索引
        createIndexOnCQEngine();
        // 插入所有测试用例到CQEngine
        testInsertToCQEngine();
        // 统计查询时间
        for (int i = 0; i < queryTimes; i++) {
            long start = System.currentTimeMillis();
            testCQEngineQuery(orderFlag);
            long cost = System.currentTimeMillis() - start;
            totalCost[2] += cost;
            maxCost[2] = Math.max(maxCost[2], cost);
        }

        // 初始化CQEngine，设置集合为TransactionalIndexedCollection
        renewCQEngineCollection(WITH_TRANSACTION);
        // 在CQEngine建立索引
        createIndexOnCQEngine();
        // 插入所有测试用例到CQEngine
        testInsertToCQEngine();
        // 统计查询时间
        for (int i = 0; i < queryTimes; i++) {
            long start = System.currentTimeMillis();
            testCQEngineQuery(orderFlag);
            long cost = System.currentTimeMillis() - start;
            totalCost[3] += cost;
            maxCost[3] = Math.max(maxCost[3], cost);
        }

        logger.info("在{}数据集大小下，重复了{}次查询，单次查询平均结果为：" +
                "SQLite（未使用索引）耗时{}ms，" +
                "SQLite（使用索引）耗时{}ms，" +
                "ConcurrentIndexedCollection耗时{}ms，" +
                "TransactionalIndexedCollection耗时{}ms",
                dataSize, queryTimes, totalCost[0] / queryTimes, totalCost[1] / queryTimes, totalCost[2] / queryTimes, totalCost[3] / queryTimes);
        logger.info("在{}数据集大小下，重复了{}次查询，单次查询最大结果为：" +
                "SQLite（未使用索引）耗时{}ms，" +
                "SQLite（使用索引）耗时{}ms，" +
                "ConcurrentIndexedCollection耗时{}ms，" +
                "TransactionalIndexedCollection耗时{}ms",
                dataSize, queryTimes, maxCost[0], maxCost[1], maxCost[2], maxCost[3]);
    }

    /**
     * 测试CQEngine并发执行读写操作
     * @param initSize 测试用例初始大小
     * @param dataSize 并发写入数据量
     * @param transactionalFlag 事务开关
     * @param writeInterval 写入时间间隔，单位ms
     * @param readInterval 查询时间间隔，单位ms
     */
    public static void testCQEngineConcurrentReadWrite(int initSize, int dataSize, boolean transactionalFlag, long writeInterval, long readInterval) {
        // 创建测试用例
        generateTestData(initSize);
        // 初始化CQEngine
        renewCQEngineCollection(transactionalFlag);
        // 在CQEngine上创建索引
        createIndexOnCQEngine();
        // 插入所有测试用例到CQEngine
        testInsertToCQEngine();

        // 创建并发写入用的测试用例
        generateTestData(dataSize);

        // 写入完成标志位
        AtomicBoolean finishFlag = new AtomicBoolean(false);

        // 测试写线程
        Thread writeThread = new Thread(() -> {

            long totalCost = 0;
            long maxCost = 0;

            for (BondData data : testData) {
                long start = System.currentTimeMillis();
                testSingleInsertToCQEngine(data);
                long cost = System.currentTimeMillis() - start;

                totalCost += cost;
                maxCost = Math.max(maxCost, cost);

                try {
                    Thread.sleep(writeInterval);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            finishFlag.set(true);

            logger.info("向CQEngine新增了{}条数据，共耗时{}ms，单次写入平均耗时{}ms，单次写入最大耗时{}ms", dataSize, totalCost, totalCost / dataSize, maxCost);
        });

        // 测试读线程
        Thread readThread = new Thread(() -> {

            long totalCost = 0;
            long maxCost = 0;
            int queryCnt = 0;

            while (finishFlag.get() == false) {
                long start = System.currentTimeMillis();
                testCQEngineQuery(WITH_ORDER);
                long cost = System.currentTimeMillis() - start;

                totalCost += cost;
                maxCost = Math.max(maxCost, cost);
                queryCnt += 1;

                try {
                    Thread.sleep(readInterval);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            logger.info("向CQEngine查询了{}次数据，共耗时{}ms，单次查询平均耗时{}ms，单次查询最大耗时{}ms", queryCnt, totalCost, totalCost / queryCnt, maxCost);
        });

        writeThread.start();
        readThread.start();

    }

    private static void testSQLiteConcurrentWrite(int initSize, int dataSize, long writeInterval) throws Exception {
        // 创建测试用例
        generateTestData(initSize);

        // 创建SQLite库表
        createSQLiteTable();
        // 插入所有测试数据到SQLite
        testInsertToSQLite();

        // 创建并发写入的测试用例
        generateTestData(dataSize);

        // 写入完成标志位
        AtomicBoolean finishFlag1 = new AtomicBoolean(false);
        AtomicBoolean finishFlag2 = new AtomicBoolean(false);

        // 测试写线程
        Thread writeThread1 = new Thread(() -> {

            long totalCost = 0;
            long maxCost = 0;

            for (BondData data : testData) {
                long start = System.currentTimeMillis();
                testSingleInsertToSQLite(data);
                long cost = System.currentTimeMillis() - start;

                totalCost += cost;
                maxCost = Math.max(maxCost, cost);

                try {
                    Thread.sleep(writeInterval);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            finishFlag1.set(true);

            logger.info("writeThread1: 向SQLite新增了{}条数据，共耗时{}ms，单次写入平均耗时{}ms，单次写入最大耗时{}ms", dataSize, totalCost, totalCost / dataSize, maxCost);
        });

        // 测试写线程
        Thread writeThread2 = new Thread(() -> {

            long totalCost = 0;
            long maxCost = 0;

            for (BondData data : testData) {
                long start = System.currentTimeMillis();
                testSingleInsertToSQLite(data);
                long cost = System.currentTimeMillis() - start;

                totalCost += cost;
                maxCost = Math.max(maxCost, cost);

                try {
                    Thread.sleep(writeInterval);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            finishFlag2.set(true);

            logger.info("writeThread2: 向SQLite新增了{}条数据，共耗时{}ms，单次写入平均耗时{}ms，单次写入最大耗时{}ms", dataSize, totalCost, totalCost / dataSize, maxCost);
        });

        writeThread1.start();
        writeThread2.start();
    }

    /**
     * testSQLiteConcurrentReadWrite(100000, 1000, 20, 50);
     * 测试SQLite并发执行读写操作
     * @param initSize 测试用例初始大小
     * @param dataSize 写入数据量
     * @param writeInterval 写入时间间隔，单位ms
     * @param readInterval 查询时间间隔，单位ms
     */
    public static void testSQLiteConcurrentReadWrite(int initSize, int dataSize, long writeInterval, long readInterval) throws Exception {
        // 创建测试用例
        generateTestData(initSize);
        // 创建SQLite库表
        createSQLiteTable();
        // 插入所有测试数据到SQLite
        testInsertToSQLite();

        // 创建并发写入的测试用例
        generateTestData(dataSize);

        // 写入完成标志位
        AtomicBoolean finishFlag = new AtomicBoolean(false);

        // 测试写线程
        Thread writeThread = new Thread(() -> {

            long totalCost = 0;
            long maxCost = 0;

            for (BondData data : testData) {
                long start = System.currentTimeMillis();
                testSingleInsertToSQLite(data);
                long cost = System.currentTimeMillis() - start;

                totalCost += cost;
                maxCost = Math.max(maxCost, cost);

                try {
                    Thread.sleep(writeInterval);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            finishFlag.set(true);

            logger.info("向SQLite新增了{}条数据，共耗时{}ms，单次写入平均耗时{}ms，单次写入最大耗时{}ms", dataSize, totalCost, totalCost / dataSize, maxCost);
        });

        // 测试读线程
        Thread readThread = new Thread(() -> {

            long totalCost = 0;
            long maxCost = 0;
            int queryCnt = 0;

            while (finishFlag.get() == false) {
                long start = System.currentTimeMillis();
                testSQLiteQuery(WITH_ORDER);
                long cost = System.currentTimeMillis() - start;

                totalCost += cost;
                maxCost = Math.max(maxCost, cost);
                queryCnt += 1;

                try {
                    Thread.sleep(readInterval);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            logger.info("向SQLite查询了{}次数据，共耗时{}ms，单次查询平均耗时{}ms，单次查询最大耗时{}ms", queryCnt, totalCost, totalCost / queryCnt, maxCost);
        });

        writeThread.start();
        readThread.start();
    }

    /**
     * 测试聚合查询
     * @param dataSize 测试用例大小
     * @param queryTimes 查询次数
     */
    public static void testAggregationQuery(int dataSize, int queryTimes) throws Exception {
        // 创建测试用例
        generateTestData(dataSize);

        // 计时器 1.SQLite（未建立索引） 2.SQLite（建立索引） 3.CQEngine（ConcurrentIndexedCollection） 4.CQEngine（TransactionalIndexedCollection）
        long[] totalCost = new long[4];
        long[] maxCost = new long[4];

        // 创建SQLite库表
        createSQLiteTable();
        // 插入所有测试用例到SQLite
        testInsertToSQLite();
        // 统计查询时间
        for (int i = 0; i < queryTimes; i++) {
            long start = System.currentTimeMillis();
            testSQLiteAggregationQuery();
            long cost = System.currentTimeMillis() - start;
            totalCost[0] += cost;
            maxCost[0] = Math.max(maxCost[0], cost);
        }
        // 清空SQLite
        clearSQLite();

        // 初始化CQEngine，设置集合为ConcurrentIndexedCollection
        renewCQEngineCollection(WITHOUT_TRANSACTION);
        // 在CQEngine建立索引
        createIndexOnCQEngine();
        // 插入所有测试用例到CQEngine
        testInsertToCQEngine();
        // 统计查询时间
        for (int i = 0; i < queryTimes; i++) {
            long start = System.currentTimeMillis();
            testCQEngineAggregation();
            long cost = System.currentTimeMillis() - start;
            totalCost[2] += cost;
            maxCost[2] = Math.max(maxCost[2], cost);
        }

        // 初始化CQEngine，设置集合为TransactionalIndexedCollection
        renewCQEngineCollection(WITH_TRANSACTION);
        // 在CQEngine建立索引
        createIndexOnCQEngine();
        // 插入所有测试用例到CQEngine
        testInsertToCQEngine();
        // 统计查询时间
        for (int i = 0; i < queryTimes; i++) {
            long start = System.currentTimeMillis();
            testCQEngineAggregation();
            long cost = System.currentTimeMillis() - start;
            totalCost[3] += cost;
            maxCost[3] = Math.max(maxCost[3], cost);
        }

        logger.info("在{}数据集大小下，重复了{}次聚合查询，单次查询平均结果为：" +
                        "SQLite（未使用索引）耗时{}ms，" +
                        "ConcurrentIndexedCollection耗时{}ms，" +
                        "TransactionalIndexedCollection耗时{}ms",
                dataSize, queryTimes, totalCost[0] / queryTimes, totalCost[2] / queryTimes, totalCost[3] / queryTimes);
        logger.info("在{}数据集大小下，重复了{}次聚合查询，单次查询最大结果为：" +
                        "SQLite（未使用索引）耗时{}ms，" +
                        "ConcurrentIndexedCollection耗时{}ms，" +
                        "TransactionalIndexedCollection耗时{}ms",
                dataSize, queryTimes, maxCost[0], maxCost[2], maxCost[3]);
    }


    /**
     * 测试聚合查询
     * @param dataSize 主表测试用例大小
     * @param extraSize 关联表测试用例大小
     * @param queryTimes 查询次数
     */
    public static void testJoinQuery(int dataSize, int extraSize, int queryTimes) throws Exception {
        // 创建测试用例
        generateExtraTestData(dataSize, extraSize);

        // 计时器 1.SQLite 3.CQEngine（ConcurrentIndexedCollection） 4.CQEngine（TransactionalIndexedCollection）
        long[] totalCost = new long[4];
        long[] maxCost = new long[4];

        // 创建SQLite主表
        createSQLiteTable();
        // 创建SQLite关联表
        createSQLiteExtraTable();
        // 插入所有测试用例到SQLite
        testExtraInsertToSQLite();
        // 统计查询时间
        for (int i = 0; i < queryTimes; i++) {
            long start = System.currentTimeMillis();
            testSQLiteJoinQuery();
            long cost = System.currentTimeMillis() - start;
            totalCost[0] += cost;
            maxCost[0] = Math.max(maxCost[0], cost);
        }
        // 清空SQLite
        clearSQLite();

        // 初始化CQEngine，设置主表集合、关联表集合都为ConcurrentIndexedCollection
        renewCQEngineCollection(WITHOUT_TRANSACTION);
        renewExtraCQEngineCollection(WITHOUT_TRANSACTION);
        // 在CQEngine建立索引
        createIndexOnCQEngine();
        createExtraIndexOnCQEngine();
        // 插入所有测试用例到CQEngine
        testExtraInsertToCQEngine();
        // 统计查询时间
        for (int i = 0; i < queryTimes; i++) {
            long start = System.currentTimeMillis();
            testCQEngineJoinQuery();
            long cost = System.currentTimeMillis() - start;
            totalCost[2] += cost;
            maxCost[2] = Math.max(maxCost[2], cost);
        }

        // 初始化CQEngine，设置集合为TransactionalIndexedCollection
        renewCQEngineCollection(WITH_TRANSACTION);
        renewExtraCQEngineCollection(WITH_TRANSACTION);
        // 在CQEngine建立索引
        createIndexOnCQEngine();
        createExtraIndexOnCQEngine();
        // 插入所有测试用例到CQEngine
        testExtraInsertToCQEngine();
        // 统计查询时间
        for (int i = 0; i < queryTimes; i++) {
            long start = System.currentTimeMillis();
            testCQEngineJoinQuery();
            long cost = System.currentTimeMillis() - start;
            totalCost[3] += cost;
            maxCost[3] = Math.max(maxCost[3], cost);
        }

        logger.info("在{}数据集大小下，重复了{}次关联查询，单次查询平均结果为：" +
                        "SQLite耗时{}ms，" +
                        "ConcurrentIndexedCollection耗时{}ms，" +
                        "TransactionalIndexedCollection耗时{}ms",
                dataSize, queryTimes, totalCost[0] / queryTimes, totalCost[2] / queryTimes, totalCost[3] / queryTimes);
        logger.info("在{}数据集大小下，重复了{}次关联查询，单次查询最大结果为：" +
                        "SQLite耗时{}ms，" +
                        "ConcurrentIndexedCollection耗时{}ms，" +
                        "TransactionalIndexedCollection耗时{}ms",
                dataSize, queryTimes, maxCost[0], maxCost[2], maxCost[3]);
    }

    /**
     * 初始化CQEngine
     * @param transactionFlag 事务开关
     */
    private static void renewCQEngineCollection(boolean transactionFlag) {
        if (transactionFlag == WITH_TRANSACTION) {
            list = new TransactionalIndexedCollection<>(BondData.class);
        } else {
            list = new ConcurrentIndexedCollection<>();
        }
    }

    /**
     * 初始化测试用例
     * @param size 测试用例大小
     */
    private static void generateTestData(int size) {

        testData = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            testData.add(new BondData(
                    contributorID[RandomUtils.nextInt(0, 5)],
                    bondName[RandomUtils.nextInt(0, 10)],
                    Float.valueOf(String.valueOf(RandomUtils.nextDouble(1, 100)).substring(0, 5)),
                    Float.valueOf(String.valueOf(RandomUtils.nextDouble(1, 100)).substring(0, 5)),
                    multiBidVolume[RandomUtils.nextInt(1, 7)],
                    askVolume[RandomUtils.nextInt(0, 7)],
                    securityID[RandomUtils.nextInt(0, 10)],
                    displayListedMarket[RandomUtils.nextInt(0, 3)],
                    marketDataTime[RandomUtils.nextInt(0, 10)]
            ));
        }
    }



    /**
     * 在CQEngine上创建索引
     */
    private static void createIndexOnCQEngine() {
        list.addIndex(HashIndex.onAttribute(BondData.CONTRIBUTOR_ID));
        list.addIndex(HashIndex.onAttribute(BondData.BOND_NAME));
        list.addIndex(NavigableIndex.onAttribute(BondData.BID_PX));
        list.addIndex(NavigableIndex.onAttribute(BondData.OFFER_PX));
        list.addIndex(NavigableIndex.onAttribute(BondData.MULTIBID_VOLUMN));
        list.addIndex(NavigableIndex.onAttribute(BondData.ASK_VOLUME));
        list.addIndex(HashIndex.onAttribute(BondData.SECURITY_ID));
        list.addIndex(HashIndex.onAttribute(BondData.DISPLAYLISTED_MARKET));
        list.addIndex(NavigableIndex.onAttribute(BondData.MARKETDATA_TIME));
    }

    /**
     * 创建SQLite库表
     */
    private static void createSQLiteTable() {
        Statement statement = null;
        try {
            // 创建表
            statement = connection.createStatement();
            String createTableQuery = "CREATE TABLE bond_data (contributorID TEXT," +
                    " bondName TEXT," +
                    "bidPx float," +
                    "offerPx float," +
                    " multiBidVolume TEXT," +
                    "askVolume float," +
                    " securityID TEXT," +
                    " displayListedMarket TEXT," +
                    " marketDataTime TEXT," +
                    " relatedID INTEGER);";
            statement.executeUpdate(createTableQuery);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 清空SQLite表数据
     */
    private static void clearSQLite() {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            String deleteQuery = "DELETE FROM bond_data";
            statement.execute(deleteQuery);
            logger.info("SQLite表数据已清空");
        } catch (Exception e) {
            logger.info(e.getMessage());
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (Exception e) {
                    logger.info(e.getMessage());
                }
            }
        }
    }

    /**
     * 在SQLite表上创建索引
     */
    private static void createIndexOnSQLite() {
        Statement statement = null;
        try {
            statement = connection.createStatement();

            // 对contributorID、bondName、securityID三个字段添加索引
            String createIndexQuery = "CREATE INDEX index_contributorID ON bond_data(contributorID);" +
                    "CREATE INDEX index_bondName ON bond_data(bondName);" +
                    "CREATE INDEX index_securityID ON bond_data(securityID);";
            statement.execute(createIndexQuery);
            logger.info("为SQLite bond_data表的contributorID, bondName和securityID添加了索引");
        } catch (Exception e) {
            logger.info(e.getMessage());
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (Exception e) {
                    logger.info(e.getMessage());
                }
            }
        }
    }

    /**
     * 测试插入数据到CQEngine
     */
    private static void testInsertToCQEngine() {
        logger.info("CQEngine开始插入数据");
        long beginTime = System.currentTimeMillis();
        for (BondData data : testData) {
            list.add(data);
        }
        logger.info("CQEngine插入数据结束，集合大小：{}，占用内存：{}，耗时：{}ms", list.size(), RamUsageEstimator.humanSizeOf(list), System.currentTimeMillis() - beginTime);
    }

    /**
     * 测试批量插入数据到SQLite
     * @param size
     * @throws Exception
     */
    private static void testInsertToSQLiteByBatch(int size) throws Exception {

        List<List<BondData>> batches = partition(testData, size);

        // 插入数据
        PreparedStatement insert = connection.prepareStatement("INSERT INTO bond_data (contributorID, " +
                "bondName, " +
                "bidPx, " +
                "offerPx, " +
                "multiBidVolume, " +
                "askVolume, " +
                "securityID, " +
                "displayListedMarket, " +
                "marketDataTime) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");

        try {
            int count = 0;

            long beginTime = System.currentTimeMillis();

            for (List<BondData> dataList : batches) {
                insert.clearParameters();
                for (BondData entity : dataList) {
                    insert.setString(1, entity.getContributorID());
                    insert.setString(2, entity.getBondName());
                    insert.setDouble(3, entity.getBidPx());
                    insert.setDouble(4, entity.getOfferPx());
                    insert.setString(5, entity.getMultiBidVolume());
                    insert.setDouble(6, entity.getAskVolume());
                    insert.setString(7, entity.getSecurityID());
                    insert.setString(8, entity.getDisplayListedMarket());
                    insert.setString(9, entity.getMarketDataTime());
                    insert.addBatch(); // 添加到批处理
                }
                insert.executeBatch();
                logger.info("批量插入{}条，批次：{}", size, ++count);
            }

            logger.info("批量插入数据到SQLite结束，耗时：{}ms", System.currentTimeMillis() - beginTime);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            if (insert != null) {
                try {
                    insert.close();
                } catch (Exception e) {
                    logger.error(e.getMessage());
                }
            }
        }
    }

    /**
     * 测试插入数据到SQLite
     * @throws Exception
     */
    private static void testInsertToSQLite() throws Exception {
        PreparedStatement insert = connection.prepareStatement("INSERT INTO bond_data (contributorID, " +
                "bondName, " +
                "bidPx, " +
                "offerPx, " +
                "multiBidVolume, " +
                "askVolume, " +
                "securityID, " +
                "displayListedMarket, " +
                "marketDataTime) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
        try {
            long beginTime = System.currentTimeMillis();

            for (BondData entity : testData) {
                insert.setString(1, entity.getContributorID());
                insert.setString(2, entity.getBondName());
                insert.setDouble(3, entity.getBidPx());
                insert.setDouble(4, entity.getOfferPx());
                insert.setString(5, entity.getMultiBidVolume());
                insert.setDouble(6, entity.getAskVolume());
                insert.setString(7, entity.getSecurityID());
                insert.setString(8, entity.getDisplayListedMarket());
                insert.setString(9, entity.getMarketDataTime());
                insert.execute();
            }

            logger.info("插入数据到SQLite结束，耗时：{}ms", System.currentTimeMillis() - beginTime);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            if (insert != null) {
                try {
                    insert.close();
                } catch (Exception e) {
                    logger.error(e.getMessage());
                }
            }
        }
    }

    /**
     * 分割数据
     * @param collection 数据集合
     * @param size 每批数据容量
     * @return
     * @param <T> 数据类型
     */
    private static <T> List<List<T>> partition(Collection<T> collection, int size) {
        List<List<T>> resultList = new ArrayList<>();
        Iterator<T> iterator = collection.iterator();

        while (iterator.hasNext()) {
            List<T> subList = new ArrayList<>();
            for (int i = 0; i < size && iterator.hasNext(); i++) {
                subList.add(iterator.next());
            }
            resultList.add(subList);
        }

        return resultList;
    }

    /**
     * 测试CQEngine查询
     * @param orderFlag 排序开关
     */
    public static void testCQEngineQuery(boolean orderFlag) {
        /** cqengine条件查询 */
        And<BondData> query = and(
                or(equal(BondData.BOND_NAME, "19四川债37")
                        , equal(BondData.BOND_NAME, "22进出05"), equal(BondData.BOND_NAME, "21工商银行二级02")),
                or(equal(BondData.SECURITY_ID, "220215")
                        , equal(BondData.SECURITY_ID, "012282297")),
                or(equal(BondData.CONTRIBUTOR_ID, "TPSH")
                        , equal(BondData.CONTRIBUTOR_ID, "CNEX")),
                greaterThan(BondData.BID_PX, 25.0f),
                lessThan(BondData.OFFER_PX, 50.0f),
                greaterThan(BondData.MULTIBID_VOLUMN, "2999"),
                between(BondData.ASK_VOLUME, 999.0f, 3001.0f),
                greaterThan(BondData.MARKETDATA_TIME, "2022-12-07"),
                lessThan(BondData.MARKETDATA_TIME, "2022-12-10")
        );

        if (orderFlag == WITH_ORDER) {
            long now = System.currentTimeMillis();
            ResultSet<BondData> resultSet = list.retrieve(query, queryOptions(orderBy(descending(missingLast(BondData.MARKETDATA_TIME)))));
            logger.info("CQEngine查询，并按时间排序，结果大小:{}，耗时：{}ms", resultSet.size(), System.currentTimeMillis() - now);
            resultSet.close();
            //logger.info("CQEngine查询，并按时间排序，耗时：{}ms",  System.currentTimeMillis() - now);
        } else {
            long now = System.currentTimeMillis();
            ResultSet<BondData> resultSet = list.retrieve(query);
            logger.info("CQEngine查询，未排序，结果大小:{}，耗时：{}ms", resultSet.size(), System.currentTimeMillis() - now);
            resultSet.close();
            //logger.info("CQEngine查询，未排序，耗时：{}ms", System.currentTimeMillis() - now);
        }
    }

    /**
     * 测试CQEngine类SQL查询
     * @param orderFlag 排序开关
     */
    public static void testCQEngineQueryBySQLParser(boolean orderFlag) {
        /** cqengine类sql查询 */
        SQLParser<BondData> parser = new SQLParser(BondData.class) {{
            registerAttribute(BondData.BOND_NAME);
            registerAttribute(BondData.SECURITY_ID);
            registerAttribute(BondData.CONTRIBUTOR_ID);
            registerAttribute(BondData.DISPLAYLISTED_MARKET);
            registerAttribute(BondData.BID_PX);
            registerAttribute(BondData.OFFER_PX);
            registerAttribute(BondData.ASK_VOLUME);
            registerAttribute(BondData.MULTIBID_VOLUMN);
            registerAttribute(BondData.MARKETDATA_TIME);
        }};

        if (orderFlag == WITH_ORDER) {
            String sql = "select * from list where (" +
                    "bondName in ('19四川债37','22进出05','21工商银行二级02') " +
                    "and securityID in ('220215','012282297') " +
                    "and contributorID in ('TPSH','CNEX') " +
                    "and multiBidVolume >'2999' " +
                    "and bidPx >25.0 " +
                    "and offerPx <50.0 " +
                    "and askVolume >999.0 " +
                    "and askVolume <3001.0 " +
                    "and marketDataTime > '2022-12-07' " +
                    "and marketDataTime < '2022-12-10' " +
                    ") order by marketDataTime desc";
            long now = System.currentTimeMillis();
            ResultSet<BondData> sqlResult = parser.retrieve(list, sql);
            logger.info("cqengine类SQL查询，并按时间排序，结果大小:{}，耗时：{}ms", sqlResult.size(), System.currentTimeMillis() - now);
        } else {
            String sql = "select * from list where " +
                    "bondName in ('19四川债37','22进出05','21工商银行二级02') " +
                    "and securityID in ('220215','012282297') " +
                    "and contributorID in ('TPSH','CNEX') " +
                    "and multiBidVolume >'2999' " +
                    "and bidPx >25.0 " +
                    "and offerPx <50.0 " +
                    "and askVolume >999.0 " +
                    "and askVolume <3001.0 " +
                    "and marketDataTime > '2022-12-07' " +
                    "and marketDataTime < '2022-12-10';";
            long now = System.currentTimeMillis();
            ResultSet<BondData> sqlResult = parser.retrieve(list, sql);
            logger.info("cqengine类SQL查询，未，结果大小:{}，耗时：{}ms", sqlResult.size(), System.currentTimeMillis() - now);
        }
    }

    /**
     * 测试SQLite查询
     * @param orderFlag 排序开关
     */
    public static void testSQLiteQuery(boolean orderFlag) {

        Statement statement = null;

        try {
            if (orderFlag == WITH_ORDER) {
                String sqliteSql="select * from bond_data where (" +
                        "bondName in ('19四川债37','22进出05','21工商银行二级02') " +
                        "and securityID in ('220215','012282297') " +
                        "and contributorID in ('TPSH','CNEX') " +
                        "and multiBidVolume >'2999' " +
                        "and bidPx >25.0 " +
                        "and offerPx <50.0 " +
                        "and askVolume >999.0 " +
                        "and askVolume <3001.0 " +
                        "and marketDataTime > '2022-12-07' " +
                        "and marketDataTime < '2022-12-10' " +
                        ") order by marketDataTime desc";
                statement = connection.createStatement();
                long now = System.currentTimeMillis();
                java.sql.ResultSet set = statement.executeQuery(sqliteSql);
                int rowCount = 0;
                while (set.next()) {
                    rowCount++;
                }
                logger.info("Sqlite查询，结果按时间排序，结果大小:{}，耗时：{}ms", rowCount, System.currentTimeMillis() - now);
                //ogger.info("Sqlite查询，结果按时间排序，耗时：{}ms", System.currentTimeMillis() - now);
            } else {
                String sqliteSql ="select * from bond_data where " +
                        "bondName in ('19四川债37','22进出05','21工商银行二级02') " +
                        "and securityID in ('220215','012282297') " +
                        "and contributorID in ('TPSH','CNEX') " +
                        "and multiBidVolume >'2999' " +
                        "and bidPx >25.0 " +
                        "and offerPx <50.0 " +
                        "and askVolume >999.0 " +
                        "and askVolume <3001.0 " +
                        "and marketDataTime > '2022-12-07' " +
                        "and marketDataTime < '2022-12-10';";;

                statement = connection.createStatement();
                long now = System.currentTimeMillis();
                java.sql.ResultSet set = statement.executeQuery(sqliteSql);
                int rowCount = 0;
                while (set.next()) {
                    rowCount++;
                }
                logger.info("Sqlite查询，未排序，结果大小:{}，耗时：{}ms", rowCount, System.currentTimeMillis() - now);
                //logger.info("Sqlite查询，未排序，耗时：{}ms", System.currentTimeMillis() - now);
            }
        } catch (Exception e) {
            logger.info(e.getMessage());
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (Exception e) {
                    logger.info(e.getMessage());
                }
            }
        }
    }

    /**
     * 向CQEngine中插入单条数据
     * @param data 测试数据
     */
    public static void testSingleInsertToCQEngine(BondData data) {
        list.add(data);
    }

    /**
     * 向SQLite中插入单条数据
     * @param data 测试数据
     */
    public static void testSingleInsertToSQLite(BondData data) {

        PreparedStatement insert = null;

        try {
            insert = connection.prepareStatement("INSERT INTO bond_data (contributorID, " +
                    "bondName, " +
                    "bidPx, " +
                    "offerPx, " +
                    "multiBidVolume, " +
                    "askVolume, " +
                    "securityID, " +
                    "displayListedMarket, " +
                    "marketDataTime) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");

            insert.setString(1, data.getContributorID());
            insert.setString(2, data.getBondName());
            insert.setDouble(3, data.getBidPx());
            insert.setDouble(4, data.getOfferPx());
            insert.setString(5, data.getMultiBidVolume());
            insert.setDouble(6, data.getAskVolume());
            insert.setString(7, data.getSecurityID());
            insert.setString(8, data.getDisplayListedMarket());
            insert.setString(9, data.getMarketDataTime());
            insert.execute();

        } catch (Exception e) {
            logger.info(e.getMessage());
        } finally {
            if (insert != null) {
                try {
                    insert.close();
                } catch (Exception e) {
                    logger.info(e.getMessage());
                }
            }
        }
    }

    /**
     * 测试SQLite聚合查询
     */
    private static void testSQLiteAggregationQuery() {

        Statement statement = null;

        try {
            String sqliteSql = "select distinct contributorID " +
                    "from bond_data " +
                    "where bondName in ('19四川债37','22进出05','21工商银行二级02') " +
                    "and securityID in ('220215','012282297') " +
                    "and contributorID in ('TPSH','CNEX') " +
                    "and multiBidVolume >'2999' " +
                    "and bidPx >25.0 " +
                    "and offerPx <50.0 " +
                    "and askVolume >999.0 " +
                    "and askVolume <3001.0 " +
                    "and marketDataTime > '2022-12-07' " +
                    "and marketDataTime < '2022-12-10' " +
                    "group by contributorID";

            statement = connection.createStatement();
            long now = System.currentTimeMillis();
            java.sql.ResultSet set = statement.executeQuery(sqliteSql);
            int rowCount = 0;
            while (set.next()) {
                rowCount++;
            }
            logger.info("Sqlite聚合查询，结果大小:{}，耗时：{}ms", rowCount, System.currentTimeMillis() - now);
        } catch (Exception e) {
            logger.info(e.getMessage());
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (Exception e) {
                    logger.info(e.getMessage());
                }
            }
        }

    }

    /**
     * 测试CQEngine聚合查询
     */
    private static void testCQEngineAggregation() {

        //CQEngine本身不包含聚合操作，若要通过CQEngine实现聚合操作，需要依赖stream流

        And<BondData> query = and(
                or(equal(BondData.BOND_NAME, "19四川债37")
                        , equal(BondData.BOND_NAME, "22进出05"), equal(BondData.BOND_NAME, "21工商银行二级02")),
                or(equal(BondData.SECURITY_ID, "220215")
                        , equal(BondData.SECURITY_ID, "012282297")),
                or(equal(BondData.CONTRIBUTOR_ID, "TPSH")
                        , equal(BondData.CONTRIBUTOR_ID, "CNEX")),
                greaterThan(BondData.BID_PX, 25.0f),
                lessThan(BondData.OFFER_PX, 50.0f),
                greaterThan(BondData.MULTIBID_VOLUMN, "2999"),
                between(BondData.ASK_VOLUME, 999.0f, 3001.0f),
                greaterThan(BondData.MARKETDATA_TIME, "2022-12-07"),
                lessThan(BondData.MARKETDATA_TIME, "2022-12-10")
        );

        long start = System.currentTimeMillis();
        ResultSet<BondData> result = list.retrieve(query);
        Set<String> set = result.stream().map(BondData::getContributorID).collect(Collectors.toSet());
        result.close();
        logger.info("CQEngine聚合查询，结果大小:{}，耗时：{}ms", set.size(), System.currentTimeMillis() - start);

    }

    /**
     * 创建SQLite关联表
     */
    private static void createSQLiteExtraTable() {
        Statement statement = null;
        try {
            // 创建表
            statement = connection.createStatement();
            String createTableQuery = "create table bond_extra_data (" +
                    "relatedID integer primary key," +
                    "testName text," +
                    "testIntro text," +
                    "testCreateTime text," +
                    "testUpdateTime text);";
            statement.executeUpdate(createTableQuery);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 初始化CQEngine关联表
     * @param transactionFlag 事务开关
     */
    private static void renewExtraCQEngineCollection(boolean transactionFlag) {
        if (transactionFlag == WITH_TRANSACTION) {
            extraList = new TransactionalIndexedCollection<>(BondExtraData.class);
        } else {
            extraList = new ConcurrentIndexedCollection<>();
        }
    }

    /**
     * 在CQEngine关联表上创建索引
     */
    private static void createExtraIndexOnCQEngine() {
        extraList.addIndex(UniqueIndex.onAttribute(BondExtraData.RELATED_ID));
        extraList.addIndex(HashIndex.onAttribute(BondExtraData.TEST_NAME));
        extraList.addIndex(NavigableIndex.onAttribute(BondExtraData.TEST_CREATE_TIME));
        extraList.addIndex(NavigableIndex.onAttribute(BondExtraData.TEST_UPDATE_TIME));

        list.addIndex(HashIndex.onAttribute(BondData.RELATED_ID));
    }

    /**
     * 初始化关联表用测试用例
     * @param mainSize 主表测试用例大小
     * @param extraSize 关联表测试用例大小
     */
    private static void generateExtraTestData(int mainSize, int extraSize) {

        testData = new ArrayList<>();

        for (int i = 0; i < mainSize; i++) {
            testData.add(new BondData(
                    contributorID[RandomUtils.nextInt(0, 5)],
                    bondName[RandomUtils.nextInt(0, 10)],
                    Float.valueOf(String.valueOf(RandomUtils.nextDouble(1, 100)).substring(0, 5)),
                    Float.valueOf(String.valueOf(RandomUtils.nextDouble(1, 100)).substring(0, 5)),
                    multiBidVolume[RandomUtils.nextInt(1, 7)],
                    askVolume[RandomUtils.nextInt(0, 7)],
                    securityID[RandomUtils.nextInt(0, 10)],
                    displayListedMarket[RandomUtils.nextInt(0, 3)],
                    marketDataTime[RandomUtils.nextInt(0, 10)],
                    RandomUtils.nextInt(0, extraSize)
            ));
        }

        testExtraData = new ArrayList<>();

        for (int i = 0; i < extraSize; i++) {
            testExtraData.add(new BondExtraData(i,
                    testName[RandomUtils.nextInt(0, testName.length)],
                    testIntro[RandomUtils.nextInt(0, testIntro.length)],
                    testCreateTime[RandomUtils.nextInt(0, testCreateTime.length)],
                    testUpdateTime[RandomUtils.nextInt(0, testUpdateTime.length)]));
        }

    }

    /**
     * 测试插入数据到CQEngine主表和关联表
     */
    private static void testExtraInsertToCQEngine() {
        logger.info("CQEngine开始插入数据");
        long beginTime = System.currentTimeMillis();
        for (BondData data : testData) {
            list.add(data);
        }
        for (BondExtraData data : testExtraData) {
            extraList.add(data);
        }
        logger.info("CQEngine插入数据到主表、关联表结束，主表集合大小：{}，关联表集合大小：{}，占用内存：{}，耗时：{}ms", list.size(), extraList.size(), RamUsageEstimator.humanSizeOf(list), System.currentTimeMillis() - beginTime);
    }

    /**
     * 测试插入数据到SQLite
     * @throws Exception
     */
    private static void testExtraInsertToSQLite() throws Exception {
        PreparedStatement insert = connection.prepareStatement("INSERT INTO bond_data (contributorID, " +
                "bondName, " +
                "bidPx, " +
                "offerPx, " +
                "multiBidVolume, " +
                "askVolume, " +
                "securityID, " +
                "displayListedMarket, " +
                "marketDataTime," +
                "relatedID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

        PreparedStatement extraInsert = connection.prepareStatement("INSERT INTO bond_extra_data (" +
                "relatedID," +
                "testName," +
                "testIntro," +
                "testCreateTime," +
                "testUpdateTime) VALUES (?, ?, ?, ?, ?)");

        try {
            long beginTime = System.currentTimeMillis();

            for (BondData entity : testData) {
                insert.setString(1, entity.getContributorID());
                insert.setString(2, entity.getBondName());
                insert.setDouble(3, entity.getBidPx());
                insert.setDouble(4, entity.getOfferPx());
                insert.setString(5, entity.getMultiBidVolume());
                insert.setDouble(6, entity.getAskVolume());
                insert.setString(7, entity.getSecurityID());
                insert.setString(8, entity.getDisplayListedMarket());
                insert.setString(9, entity.getMarketDataTime());
                insert.setInt(10, entity.getRelatedID());
                insert.execute();
            }

            for (BondExtraData entity : testExtraData) {
                extraInsert.setInt(1, entity.getRelatedID());
                extraInsert.setString(2, entity.getTestName());
                extraInsert.setString(3, entity.getTestIntro());
                extraInsert.setString(4, entity.getTestCreateTime());
                extraInsert.setString(5, entity.getTestUpdateTime());
                extraInsert.execute();
            }

            logger.info("插入数据到SQLite主表和关联表结束，耗时：{}ms", System.currentTimeMillis() - beginTime);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            if (insert != null) {
                try {
                    insert.close();
                } catch (Exception e) {
                    logger.error(e.getMessage());
                }
            }
        }
    }

    /**
     * 测试SQLite关联查询
     */
    private static void testSQLiteJoinQuery() {

        Statement statement = null;

        try {

            String sqliteSql = "select " +
                    "bd.bondName as bondName, " +
                    "bed.testName as testName " +
                    "from bond_data bd " +
                    "left join bond_extra_data bed " +
                    "on bd.relatedID = bed.relatedID " +
                    "where bondName in ('19四川债37','22进出05','21工商银行二级02') " +
                    "and securityID in ('220215','012282297') " +
                    "and contributorID in ('TPSH','CNEX') " +
                    "and multiBidVolume >'2999' " +
                    "and bidPx >25.0  " +
                    "and offerPx <50.0 " +
                    "and askVolume >999.0 " +
                    "and askVolume <3001.0 " +
                    "and marketDataTime > '2022-12-07' " +
                    "and marketDataTime < '2022-12-10'";
            statement = connection.createStatement();
            long now = System.currentTimeMillis();
            java.sql.ResultSet set = statement.executeQuery(sqliteSql);
            int rowCount = 0;
            while (set.next()) {
                rowCount++;
            }
            logger.info("Sqlite关联查询，结果大小:{}，耗时：{}ms", rowCount, System.currentTimeMillis() - now);

        } catch (Exception e) {
            logger.info(e.getMessage());
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (Exception e) {
                    logger.info(e.getMessage());
                }
            }
        }
    }

    /**
     * 测试CQEngine关联查询
     */
    public static void testCQEngineJoinQuery() {
        /** cqengine条件查询 */
        And<BondData> query = and(
                or(equal(BondData.BOND_NAME, "19四川债37")
                        , equal(BondData.BOND_NAME, "22进出05"), equal(BondData.BOND_NAME, "21工商银行二级02")),
                or(equal(BondData.SECURITY_ID, "220215")
                        , equal(BondData.SECURITY_ID, "012282297")),
                or(equal(BondData.CONTRIBUTOR_ID, "TPSH")
                        , equal(BondData.CONTRIBUTOR_ID, "CNEX")),
                greaterThan(BondData.BID_PX, 25.0f),
                lessThan(BondData.OFFER_PX, 50.0f),
                greaterThan(BondData.MULTIBID_VOLUMN, "2999"),
                between(BondData.ASK_VOLUME, 999.0f, 3001.0f),
                greaterThan(BondData.MARKETDATA_TIME, "2022-12-07"),
                lessThan(BondData.MARKETDATA_TIME, "2022-12-10")
        );

        long now = System.currentTimeMillis();
        // 先获取主表结果
        ResultSet<BondData> resultSet = list.retrieve(query);
        List<BondMixedData> res = new ArrayList<>();
        // 遍历主表结果，对每个结果，按照关联字段，在关联表中查询
        for (BondData entity : resultSet) {
            Query<BondExtraData> joinQuery = equal(BondExtraData.RELATED_ID, entity.getRelatedID());
            // 此时ResultSet中可能会包含多个对象，但遍历ResultSet实际上只会获得一个对象，原理详见CQEngine源码
            ResultSet<BondExtraData> joinResult = extraList.retrieve(joinQuery);
            BondExtraData joinEntity = new BondExtraData();
            for (BondExtraData data : joinResult) {
                joinEntity = data;
            }

            BondMixedData mixedData = new BondMixedData(entity.getBondName(), entity.getRelatedID(), joinEntity.getTestName());
            res.add(mixedData);
        }
        logger.info("CQEngine关联查询，结果大小:{}，耗时：{}ms", res.size(), System.currentTimeMillis() - now);
        resultSet.close();
    }






    /**
     * 测试CQEngine在写入时查询
     * @param orderFlag 排序开关
     * @param queryInterval 每次查询请求间隔，单位ms
     */
    public static void testCQEngineConcurrentOpe(boolean orderFlag, long queryInterval) {

        AtomicBoolean finishFlag = new AtomicBoolean(false);

        Thread writeThread = new Thread(() -> {
            testInsertToCQEngine();
            finishFlag.set(true);
        });

        Thread readThread = new Thread(() -> {
            long totalCost = 0;
            long maxCost = 0;
            int queryCnt = 0;

            while (finishFlag.get() == false) {
                long curr = System.currentTimeMillis();
                testCQEngineQuery(orderFlag);
                long cost = System.currentTimeMillis() - curr;

                totalCost += cost;
                queryCnt += 1;
                maxCost = Math.max(maxCost, cost);

                try {
                    Thread.sleep(queryInterval);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            logger.info("CQEngine并发查询，共查询{}次，平均每次查询耗时约{}ms，最大查询耗时约{}ms", queryCnt, totalCost / queryCnt, maxCost);
        });

        writeThread.start();
        readThread.start();

    }

    /**
     * 测试SQLite写入时查询
     * @param orderFlag 排序开关
     * @param queryInterval 每次查询请求间隔，单位ms
     */
    public static void testSQLiteConcurrentOpe(boolean orderFlag, long queryInterval) {

        AtomicBoolean finishFlag = new AtomicBoolean(false);

        Thread writeThread = new Thread(() -> {
            try {
                testInsertToSQLite();
                finishFlag.set(true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        Thread readThread = new Thread(() -> {
            long totalCost = 0;
            long maxCost = 0;
            int queryCnt = 0;
            while (finishFlag.get() == false) {
                long curr = System.currentTimeMillis();
                testSQLiteQuery(orderFlag);
                long cost = System.currentTimeMillis() - curr;

                totalCost += cost;
                queryCnt += 1;
                maxCost = Math.max(maxCost, cost);

                try {
                    Thread.sleep(queryInterval);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            logger.info("SQLite并发查询，共查询{}次，平均每次查询耗时约{}ms，最大查询耗时约{}ms", queryCnt, totalCost / queryCnt, maxCost);
        });

        writeThread.start();
        readThread.start();

    }

    public static class SqliteHelper {
        public static void init(IndexedCollection<BondData> data) {
            Statement statement = null;
            try {
                // 创建表
                statement = connection.createStatement();
                String createTableQuery = "CREATE TABLE bond_data (contributorID TEXT," +
                        " bondName TEXT," +
                        "bidPx float," +
                        "offerPx float," +
                        " multiBidVolume TEXT," +
                        "askVolume float," +
                        " securityID TEXT," +
                        " displayListedMarket TEXT," +
                        " marketDataTime TEXT)";
                statement.executeUpdate(createTableQuery);

                // 插入数据
                PreparedStatement insert = connection.prepareStatement("INSERT INTO bond_data (contributorID, " +
                        "bondName, " +
                        "bidPx, " +
                        "offerPx, " +
                        "multiBidVolume, " +
                        "askVolume, " +
                        "securityID, " +
                        "displayListedMarket, " +
                        "marketDataTime) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");

                List<List<BondData>> partition = partition(data, 1000);
                int count = 0;
                for (List<BondData> dataList : partition) {
                    insert.clearParameters();
                    for (BondData entity : dataList) {
                        insert.setString(1, entity.getContributorID());
                        insert.setString(2, entity.getBondName());
                        insert.setDouble(3, entity.getBidPx());
                        insert.setDouble(4, entity.getOfferPx());
                        insert.setString(5, entity.getMultiBidVolume());
                        insert.setDouble(6, entity.getAskVolume());
                        insert.setString(7, entity.getSecurityID());
                        insert.setString(8, entity.getDisplayListedMarket());
                        insert.setString(9, entity.getMarketDataTime());
                        insert.addBatch(); // 添加到批处理
                    }
                    insert.executeBatch();
                    logger.info("批量插入1000条，批次：{}", ++count);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                // 关闭资源
                if (statement != null) {
                    try {
                        statement.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        public static <T> List<List<T>> partition(IndexedCollection<T> collection, int size) {
            List<List<T>> resultList = new ArrayList<>();
            Iterator<T> iterator = collection.iterator();

            while (iterator.hasNext()) {
                List<T> subList = new ArrayList<>();
                for (int i = 0; i < size && iterator.hasNext(); i++) {
                    subList.add(iterator.next());
                }
                resultList.add(subList);
            }

            return resultList;
        }

    }

    public static void testQuery() throws Exception {
        // 日期
        Date date1 = DateUtils.parseDate("2022-12-07", "yyyy-MM-dd");
        Date date2 = DateUtils.parseDate("2022-12-10", "yyyy-MM-dd");
        logger.info("准备数据完毕，开始进行查询\n\n");
        Thread.sleep(3000L);

        /** for循环查询 */
        List<BondData> array = new ArrayList<>();

        long now = System.currentTimeMillis();
        for (BondData l : list) {
            if ((StringUtils.equals(l.getBondName(), "19四川债37") || StringUtils.equals(l.getBondName(), "22进出05") || StringUtils.equals(l.getBondName(), "21工商银行二级02"))
                    && (StringUtils.equals(l.getSecurityID(), "220215") || StringUtils.equals(l.getSecurityID(), "012282297"))
                    && (StringUtils.equals(l.getContributorID(), "TPSH") || StringUtils.equals(l.getContributorID(), "CNEX"))
                    && Integer.parseInt(l.getMultiBidVolume()) >= 3000
                    && l.getBidPx() > 25.0f
                    && l.getOfferPx() < 50.0f
                    && (l.getAskVolume() >= 1000.0f && l.getAskVolume() <= 3000.0f)
                    && (DateUtils.parseDate(l.getMarketDataTime(), "yyyy-MM-dd HH:mm:ss.Sss").before(date2)
                    && DateUtils.parseDate(l.getMarketDataTime(), "yyyy-MM-dd HH:mm:ss.Sss").after(date1))) {
                array.add(l);
            }
        }

        Collections.sort(array, (s1, s2) -> {
            try {
                return DateUtils.parseDate(s2.getMarketDataTime(), "yyyy-MM-dd HH:mm:ss.Sss")
                        .compareTo(DateUtils.parseDate(s1.getMarketDataTime(), "yyyy-MM-dd HH:mm:ss.Sss"));
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        });
        logger.info("for循环查询结果大小:{}，耗时：{}", array.size(), System.currentTimeMillis() - now);


        /** forEach查询 */
        now = System.currentTimeMillis();
        List<BondData> sss = list.stream().filter(l -> {
            boolean b;
            try {
                b = DateUtils.parseDate(l.getMarketDataTime(), "yyyy-MM-dd HH:mm:ss.Sss").before(date2)
                        && DateUtils.parseDate(l.getMarketDataTime(), "yyyy-MM-dd HH:mm:ss.Sss").after(date1);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            return (StringUtils.equals(l.getBondName(), "19四川债37") || StringUtils.equals(l.getBondName(), "22进出05") || StringUtils.equals(l.getBondName(), "21工商银行二级02"))
                    && (StringUtils.equals(l.getSecurityID(), "220215") || StringUtils.equals(l.getSecurityID(), "012282297"))
                    && (StringUtils.equals(l.getContributorID(), "TPSH") || StringUtils.equals(l.getContributorID(), "CNEX"))
                    && Integer.parseInt(l.getMultiBidVolume()) >= 3000
                    && l.getBidPx() > 25.0f
                    && l.getOfferPx() < 50.0f
                    && (l.getAskVolume() >= 1000.0f && l.getAskVolume() <= 3000.0f)
                    && b;
        }).collect(Collectors.toList());
        Collections.sort(sss, (s1, s2) -> {
            try {
                return DateUtils.parseDate(s2.getMarketDataTime(), "yyyy-MM-dd HH:mm:ss.Sss")
                        .compareTo(DateUtils.parseDate(s1.getMarketDataTime(), "yyyy-MM-dd HH:mm:ss.Sss"));
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        });
        logger.info("forEach查询结果大小:{}，耗时：{}", sss.size(), System.currentTimeMillis() - now);


        /** cqengine条件查询 */
        And<BondData> query = and(
                or(equal(BondData.BOND_NAME, "19四川债37")
                        , equal(BondData.BOND_NAME, "22进出05"), equal(BondData.BOND_NAME, "21工商银行二级02")),
                or(equal(BondData.SECURITY_ID, "220215")
                        , equal(BondData.SECURITY_ID, "012282297")),
                or(equal(BondData.CONTRIBUTOR_ID, "TPSH")
                        , equal(BondData.CONTRIBUTOR_ID, "CNEX")),
                greaterThan(BondData.BID_PX, 25.0f),
                lessThan(BondData.OFFER_PX, 50.0f),
                greaterThan(BondData.MULTIBID_VOLUMN, "2999"),
                between(BondData.ASK_VOLUME, 999.0f, 3001.0f),
                greaterThan(BondData.MARKETDATA_TIME, "2022-12-07"),
                lessThan(BondData.MARKETDATA_TIME, "2022-12-10")
        );
        now = System.currentTimeMillis();
        ResultSet<BondData> resultSet = list.retrieve(query, queryOptions(orderBy(descending(missingLast(BondData.MARKETDATA_TIME)))));
        logger.info("cqengine询结果大小:{}，耗时：{}", resultSet.size(), System.currentTimeMillis() - now);


        /** cqengine类sql查询 */
        SQLParser<BondData> parser = new SQLParser(BondData.class) {{
            registerAttribute(BondData.BOND_NAME);
            registerAttribute(BondData.SECURITY_ID);
            registerAttribute(BondData.CONTRIBUTOR_ID);
            registerAttribute(BondData.DISPLAYLISTED_MARKET);
            registerAttribute(BondData.BID_PX);
            registerAttribute(BondData.OFFER_PX);
            registerAttribute(BondData.ASK_VOLUME);
            registerAttribute(BondData.MULTIBID_VOLUMN);
            registerAttribute(BondData.MARKETDATA_TIME);
        }};

        String sql = "select * from list where (" +
                "bondName in ('19四川债37','22进出05','21工商银行二级02') " +
                "and securityID in ('220215','012282297') " +
                "and contributorID in ('TPSH','CNEX') " +
                "and multiBidVolume >'2999' " +
                "and bidPx >25.0 " +
                "and offerPx <50.0 " +
                "and askVolume >999.0 " +
                "and askVolume <3001.0 " +
                "and marketDataTime > '2022-12-07' " +
                "and marketDataTime < '2022-12-10' " +
                ") order by marketDataTime desc";
        now = System.currentTimeMillis();
        ResultSet<BondData> sqlResult = parser.retrieve(list, sql);
        logger.info("cqengine类SQL查询结果大小:{}，耗时：{}", sqlResult.size(), System.currentTimeMillis() - now);

        String sqliteSql="select * from bond_data where (" +
                "bondName in ('19四川债37','22进出05','21工商银行二级02') " +
                "and securityID in ('220215','012282297') " +
                "and contributorID in ('TPSH','CNEX') " +
                "and multiBidVolume >'2999' " +
                "and bidPx >25.0 " +
                "and offerPx <50.0 " +
                "and askVolume >999.0 " +
                "and askVolume <3001.0 " +
                "and marketDataTime > '2022-12-07' " +
                "and marketDataTime < '2022-12-10' " +
                ") order by marketDataTime desc";
        Statement statement = connection.createStatement();
        now = System.currentTimeMillis();
        java.sql.ResultSet set = statement.executeQuery(sqliteSql);
        int rowCount = 0;
        while (set.next()) {
            rowCount++;
        }
        logger.info("Sqlite查询结果大小:{}，耗时：{}", rowCount, System.currentTimeMillis() - now);
        close();
    }

    public static void close(){
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
