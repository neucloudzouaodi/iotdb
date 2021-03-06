package cn.edu.tsinghua.iotdb.qp.utils;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.qp.constant.SQLConstant;
import cn.edu.tsinghua.iotdb.qp.executor.QueryProcessExecutor;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.DeletePlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.InsertPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.UpdatePlan;
import cn.edu.tsinghua.iotdb.query.management.FilterStructure;
import cn.edu.tsinghua.iotdb.query.fill.IFill;
import cn.edu.tsinghua.tsfile.common.constant.SystemConstant;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.CrossSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Field;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.support.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.utils.StringContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Implement a simple executor with a memory demo reading processor for test.
 *
 * @author kangrong
 */
public class MemIntQpExecutor extends QueryProcessExecutor {
    private static Logger LOG = LoggerFactory.getLogger(MemIntQpExecutor.class);

    private SingleValueVisitor<Long> timeVisitor = new SingleValueVisitor<>();
    private SingleValueVisitor<Integer> valueVisitor = new SingleValueVisitor<>();

    //pathStr, TreeMap<time, value>
    private Map<String, TestSeries> demoMemDataBase = new HashMap<>();

    private TreeSet<Long> timeStampUnion = new TreeSet<>();

    public void setFakeAllPaths(Map<String, List<String>> fakeAllPaths) {
        this.fakeAllPaths = fakeAllPaths;
    }

    private Map<String, List<String>> fakeAllPaths;

    public MemIntQpExecutor() {
        this.fetchSize.set(5);
        // super.fetchSize = 5;
    }

    @Override
    public TSDataType getSeriesType(Path fullPath) {
        if (fullPath.equals(SQLConstant.RESERVED_TIME))
            return TSDataType.INT64;
        if (fullPath.equals(SQLConstant.RESERVED_FREQ))
            return TSDataType.FLOAT;
        if (fakeAllPaths != null && fakeAllPaths.containsKey(fullPath.toString()))
            return TSDataType.INT32;
        if (demoMemDataBase.containsKey(fullPath.toString()))
            return TSDataType.INT32;
        return null;
    }

    @Override
    public boolean processNonQuery(PhysicalPlan plan) throws ProcessorException {
        switch (plan.getOperatorType()) {
            case DELETE:
                DeletePlan delete = (DeletePlan) plan;
                return delete(delete.getPaths(), delete.getDeleteTime());
            case UPDATE:
                UpdatePlan update = (UpdatePlan) plan;
                boolean flag = true;
                for (Pair<Long, Long> timePair : update.getIntervals()) {
                    flag &= update(update.getPath(), timePair.left, timePair.right, update.getValue());
                }
                return flag;
            case INSERT:
                InsertPlan insert = (InsertPlan) plan;
                int result = multiInsert(insert.getDeltaObject(), insert.getTime(), insert.getMeasurements(), insert
                        .getValues());
                return result == 0;
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public boolean judgePathExists(Path path) {
        if (SQLConstant.isReservedPath(path))
            return true;
        if (fakeAllPaths != null) {
            return fakeAllPaths.containsKey(path.toString());
        }
        return demoMemDataBase.containsKey(path.toString());
    }

    @Override
    public boolean update(Path path, long startTime, long endTime, String value) {
        if (!demoMemDataBase.containsKey(path.toString())) {
            LOG.warn("no series:{}", path);
            return false;
        }
        TestSeries series = demoMemDataBase.get(path.toString());
        for (Entry<Long, Integer> entry : series.data.entrySet()) {
            long timestamp = entry.getKey();
            if (timestamp >= startTime && timestamp <= endTime)
                entry.setValue(Integer.valueOf(value));
        }
        LOG.info("update, series:{}, time range:<{},{}>, value:{}", path, startTime, endTime, value);
        return true;
    }

    @Override
    protected boolean delete(Path path, long deleteTime) {
        if (!demoMemDataBase.containsKey(path.toString()))
            return true;
        TestSeries series = demoMemDataBase.get(path.toString());
        TreeMap<Long, Integer> delResult = new TreeMap<>();
        for (Entry<Long, Integer> entry : series.data.entrySet()) {
            long timestamp = entry.getKey();
            if (timestamp >= deleteTime) {
                delResult.put(timestamp, entry.getValue());
            }
        }
        series.data = delResult;
        LOG.info("delete series:{}, timestamp:{}", path, deleteTime);
        return true;
    }

    @Override
    public int insert(Path path, long insertTime, String value) {
        String strPath = path.toString();
        if (!demoMemDataBase.containsKey(strPath)) {
            demoMemDataBase.put(strPath, new TestSeries());
        }
        demoMemDataBase.get(strPath).data.put(insertTime, Integer.valueOf(value));
        timeStampUnion.add(insertTime);
        LOG.info("insert into {}:<{},{}>", path, insertTime, value);
        return 0;
    }

    private Boolean satisfyValue(SingleValueVisitor<Integer> v, FilterExpression expr, String path,
                                 int value) {
        if (expr instanceof SingleSeriesFilterExpression) {
            SingleSeriesFilterExpression single = (SingleSeriesFilterExpression) expr;
            StringContainer sc = new StringContainer(SystemConstant.PATH_SEPARATOR);
            sc.addTail(single.getFilterSeries().getDeltaObjectUID(), single.getFilterSeries().getMeasurementUID());
            String filterPath = sc.toString();
            if (path.equals(filterPath)) {
                return v.satisfyObject(value, single);
            } else
                // not me, return true
                return null;
        } else {
            Boolean left = satisfyValue(v, ((CrossSeriesFilterExpression) expr).getLeft(), path, value);
            if (left != null)
                return left;
            return satisfyValue(v, ((CrossSeriesFilterExpression) expr).getLeft(), path, value);
        }
    }


    @Override
    public QueryDataSet aggregate(List<Pair<Path, String>> aggres, List<FilterStructure> filterStructures)
            throws ProcessorException, IOException, PathErrorException {
        throw new ProcessorException("Do not support");
    }

    @Override
    public QueryDataSet groupBy(List<Pair<Path, String>> aggres, List<FilterStructure> filterStructures,
                                long unit, long origin, List<Pair<Long, Long>> intervals, int fetchSize)
            throws ProcessorException, IOException, PathErrorException {
        throw new ProcessorException("Do not support");
    }

    @Override
    public QueryDataSet fill(List<Path> fillPaths, long queryTime, Map<TSDataType, IFill> fillType)
            throws ProcessorException, IOException, PathErrorException {
        throw new ProcessorException("Do not support");
    }

    /**
     * This method is just a simple implementation of read processing in memory for JUnit Test. It
     * doesn't support frequency filter.
     */
    @Override
    public QueryDataSet query(int formNumber, List<Path> paths, FilterExpression timeFilter,
                              FilterExpression freqFilter, FilterExpression valueFilter,
                              int fetchSize, QueryDataSet lastData) {
        if (fetchSize == 0) {
            LOG.error("cannot specify fetchSize to zero,exit");
            System.exit(0);
        }
        TestOutputQueryDataSet ret = new TestOutputQueryDataSet(fetchSize);
        long lastGetTimeStamp =
                (lastData == null) ? -1 : ((TestOutputQueryDataSet) lastData)
                        .getLastRowRecordTimeStamp();
        int haveSize = 0;

        SingleSeriesFilterExpression timeSingleFilter = (SingleSeriesFilterExpression) timeFilter;

        for (long time : timeStampUnion) {
            if (time <= lastGetTimeStamp)
                continue;
            if (timeFilter == null || timeVisitor.satisfyObject(time, timeSingleFilter)) {
                TestIntegerRowRecord rowRecord = new TestIntegerRowRecord(time);
                boolean isSatisfy = true;
                boolean isInputed = false;
                for (Path path : paths) {
                    String fullPath = path.toString();
                    if (!demoMemDataBase.containsKey(fullPath)) {
                        // this database has not this path
                        rowRecord.addSensor(fullPath, "null");

                    } else {
                        TestSeries ts = demoMemDataBase.get(fullPath);
                        if (ts.data.containsKey(time)) {
                            Integer v = ts.data.get(time);

                            if (valueFilter == null) {
                                // no filter
                                rowRecord.addSensor(fullPath, v.toString());
                                isInputed = true;
                                Field f = new Field(TSDataType.INT32, path.getDeltaObjectToString(), path
                                        .getMeasurementToString());
                                f.setNull(true);
                                rowRecord.addField(f);
                            } else {
                                Boolean satisfyResult =
                                        satisfyValue(valueVisitor, valueFilter, fullPath, v);
                                if (satisfyResult == null) {
                                    // not my filter, I add it but don't set inputed
                                    rowRecord.addSensor(fullPath, v.toString());
                                    Field f = new Field(TSDataType.INT32, path.getDeltaObjectToString(), path
                                            .getMeasurementToString());
                                    f.setIntV(v);
                                    rowRecord.addField(f);
                                } else if (satisfyResult) {
                                    // have filter and it's my filter,and satisfy, inputed
                                    rowRecord.addSensor(fullPath, v.toString());
                                    isInputed = true;

                                    Field f = new Field(TSDataType.INT32, path.getDeltaObjectToString(), path
                                            .getMeasurementToString());
                                    f.setIntV(v);
                                    rowRecord.addField(f);
                                } else {
                                    // have filter, and it's my filter,and not satisfy, don't
                                    // satisfy
                                    isSatisfy = false;
                                    break;
                                }
                            }
                        } else {
                            // this series has not this path
                            rowRecord.addSensor(fullPath, "null");
                            Field f = new Field(TSDataType.INT32, path.getDeltaObjectToString(), path
                                    .getMeasurementToString());
                            f.setNull(true);
                            rowRecord.addField(f);
                        }
                    }
                }
                if (isSatisfy && isInputed) {
                    haveSize++;
                    ret.addRowRecord(rowRecord);
                    if (haveSize > fetchSize)
                        break;
                }
            }
        }
        return ret;
    }

    @Override
    public List<String> getAllPaths(String fullPath) {
        return fakeAllPaths != null ? fakeAllPaths.get(fullPath) :
                new ArrayList<String>() {{
                    add(fullPath);
                }};
    }

    @Override
    public int multiInsert(String deltaObject, long insertTime, List<String> measurementList, List<String>
            insertValues) {
        return 0;
    }

    private class TestOutputQueryDataSet extends OutputQueryDataSet {

        public TestOutputQueryDataSet(int fetchSize) {
            super(fetchSize);
        }

        /**
         * return the last record's timestamp of last Set.
         *
         * @return -1 means it has not got all data.
         */
        public long getLastRowRecordTimeStamp() {
            if (size == 0)
                return -1;
            // return this.data[this.size == fetchSize ? size - 1 : size].timestamp;
            return this.data[size - 1].timestamp;
        }

    }

    /**
     * This class extends RowRecord to adapt the parameters and return type. It's just for test. It
     * provides a list of sensors integer type of string and their values in type of integer.
     *
     * @author kangrong
     */
    private class TestIntegerRowRecord extends RowRecord {
        //pair<path, value>
        public List<Pair<String, String>> measurementData = new ArrayList<>();

        public TestIntegerRowRecord(long timestamp) {
            super(timestamp, "", "");
        }

        // TODO
        public void addSensor(String path, String value) {
            measurementData.add(new Pair<>(path, value));
        }

        public void putARowRecord(RowRecord record) {
            TestIntegerRowRecord tmpRecord = (TestIntegerRowRecord) record;
            for (Pair<String, String> pair : tmpRecord.getMeasureMentData()) {
                this.addSensor(pair.left, pair.right);
            }
        }

        @Override
        public String toString() {
            StringContainer sc = new StringContainer();
            sc.addTail(Long.toString(timestamp), ", ");
            for (Pair<String, String> v : measurementData) {
                sc.addTail("<", v.left, ",", v.right, "> ");
            }
            return sc.toString();
        }

        public List<Pair<String, String>> getMeasureMentData() {
            return this.measurementData;
        }
    }

    private class TestSeries {
        public TreeMap<Long, Integer> data = new TreeMap<>();
    }
}
