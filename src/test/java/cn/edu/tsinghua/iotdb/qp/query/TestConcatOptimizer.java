package cn.edu.tsinghua.iotdb.qp.query;

import cn.edu.tsinghua.iotdb.exception.ArgsErrorException;
import cn.edu.tsinghua.iotdb.qp.QueryProcessor;
import cn.edu.tsinghua.iotdb.qp.exception.QueryProcessorException;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.qp.utils.MemIntQpExecutor;
import cn.edu.tsinghua.iotdb.qp.strategy.optimizer.ConcatPathOptimizer;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import org.antlr.runtime.RecognitionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.assertEquals;


/**
 * test the correctness of
 * {@linkplain ConcatPathOptimizer ConcatPathOptimizer}
 *
 * @author kangrong
 */
@RunWith(Parameterized.class)
public class TestConcatOptimizer {
    private static final Logger LOG = LoggerFactory.getLogger(TestConcatOptimizer.class);
    private QueryProcessor processor;

    private final String inputSQL;
    private final String retPlan;

    public TestConcatOptimizer(String inputSQL, String retPlan) {
        this.inputSQL = inputSQL;
        this.retPlan = retPlan;
    }

    @Before
    public void before() throws ProcessorException {
        MemIntQpExecutor memProcessor = new MemIntQpExecutor();
        Map<String, List<String>> fakeAllPaths = new HashMap<String, List<String>>() {{
            put("root.laptop.d1.s1", new ArrayList<String>() {{
                add("root.laptop.d1.s1");
            }});
            put("root.laptop.d1.s2", new ArrayList<String>() {{
                add("root.laptop.d1.s2");
            }});

            put("root.laptop.d2.s1", new ArrayList<String>() {{
                add("root.laptop.d2.s1");
            }});
            put("root.laptop.d2.s2", new ArrayList<String>() {{
                add("root.laptop.d2.s2");
            }});
            put("root.laptop.d3.s1", new ArrayList<String>() {{
                add("root.laptop.d3.s1");
            }});
            put("root.laptop.d3.s2", new ArrayList<String>() {{
                add("root.laptop.d3.s2");
            }});

            put("root.laptop.*.s1", new ArrayList<String>() {{
                add("root.laptop.d1.s1");
                add("root.laptop.d2.s1");
                add("root.laptop.d3.s1");
            }});
            put("root.laptop.*.s2", new ArrayList<String>() {{
                add("root.laptop.d1.s2");
                add("root.laptop.d2.s2");
                add("root.laptop.d3.s2");
            }});
        }};
        memProcessor.setFakeAllPaths(fakeAllPaths);
        processor = new QueryProcessor(memProcessor);
    }

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays
                .asList(new Object[][]{
                        {
                                "select s1 from root.laptop.* where s2 > 5",
                                "MultiQueryPlan:\n" +
                                        "SingleQueryPlan:\n" +
                                        "  paths:  [root.laptop.d1.s1, root.laptop.d2.s1, root.laptop.d3.s1]\n" +
                                        "  null\n" +
                                        "  null\n" +
                                        "  [and [root.laptop.d1.s2>5][root.laptop.d2.s2>5][root.laptop.d3.s2>5]]\n"
                        },
                        {
                                "select s1 from root.laptop.*, root.laptop.d2 where s2 > 5 and time < 5 and (time > 3" +
                                        " or s1 > 10)",
                                "MultiQueryPlan:\n" +
                                        "SingleQueryPlan:\n" +
                                        "  paths:  [root.laptop.d1.s1, root.laptop.d2.s1, root.laptop.d3.s1, root" +
                                        ".laptop.d2.s1]\n" +
                                        "  [and[single:time] [time<5][time>3]]\n" +
                                        "  null\n" +
                                        "  [and [root.laptop.d1.s2>5][root.laptop.d2.s2>5][root.laptop.d3.s2>5]]\n" +
                                        "\n" +
                                        "SingleQueryPlan:\n" +
                                        "  paths:  [root.laptop.d1.s1, root.laptop.d2.s1, root.laptop.d3.s1, root" +
                                        ".laptop.d2.s1]\n" +
                                        "  [time<5]\n" +
                                        "  null\n" +
                                        "  [and [root.laptop.d1.s1>10][root.laptop.d1.s2>5][root.laptop" +
                                        ".d2.s1>10][root.laptop.d2.s2>5][root.laptop.d3.s1>10][root.laptop.d3.s2>5]]\n"
                        },
                        {
                                "select s1 from root.laptop.d1, root.laptop.d2 where s1 > 5",
                                "MultiQueryPlan:\n" +
                                        "SingleQueryPlan:\n" +
                                        "  paths:  [root.laptop.d1.s1, root.laptop.d2.s1]\n" +
                                        "  null\n" +
                                        "  null\n" +
                                        "  [and [root.laptop.d1.s1>5][root.laptop.d2.s1>5]]\n"
                        },
                        {
                                "select s1 from root.laptop.d1, root.laptop.d2 where root.laptop.d2.s2 > 10",
                                "MultiQueryPlan:\n" +
                                        "SingleQueryPlan:\n" +
                                        "  paths:  [root.laptop.d1.s1, root.laptop.d2.s1]\n" +
                                        "  null\n" +
                                        "  null\n" +
                                        "  [root.laptop.d2.s2>10]\n"
                        },
                        {
                                "select s1 from root.laptop.d1, root.laptop.d2 where s1 > 5 and time > 5 or s2 < 10",
                                "MultiQueryPlan:\n" +
                                        "SingleQueryPlan:\n" +
                                        "  paths:  [root.laptop.d1.s1, root.laptop.d2.s1]\n" +
                                        "  [time>5]\n" +
                                        "  null\n" +
                                        "  [and [root.laptop.d1.s1>5][root.laptop.d2.s1>5]]\n" +
                                        "\n" +
                                        "SingleQueryPlan:\n" +
                                        "  paths:  [root.laptop.d1.s1, root.laptop.d2.s1]\n" +
                                        "  null\n" +
                                        "  null\n" +
                                        "  [and [root.laptop.d1.s2<10][root.laptop.d2.s2<10]]\n"
                        },
                });
    }

    @Test
    public void testQueryBasic() throws QueryProcessorException, RecognitionException, ArgsErrorException {
        LOG.info("input SQL String:{}", inputSQL);
        PhysicalPlan plan = processor.parseSQLToPhysicalPlan(inputSQL);
        System.out.println(plan.printQueryPlan());
        assertEquals(retPlan, plan.printQueryPlan());
    }

}