package com.vincer.lineage.core.hive;

import com.google.gson.stream.JsonWriter;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveVariableSource;
import org.apache.hadoop.hive.conf.VariableSubstitution;

import org.apache.hadoop.hive.metastore.HiveMetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.*;
import org.apache.hadoop.hive.ql.exec.FetchTask;

import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.HookUtils;
import org.apache.hadoop.hive.ql.hooks.PrivateHookContext;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.optimizer.lineage.LineageCtx;
import org.apache.hadoop.hive.ql.parse.*;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.processors.SetProcessor;
import org.apache.hadoop.hive.ql.session.LineageState;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.service.rpc.thrift.THandleIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.CommonDataSource;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryPlan;


public class LineageInfo {
    private static final String KEY_ADD = "add ";
    private static final String KEY_DROP = "drop ";
    private static final String KEY_SET = "set ";
    private static final String KEY_FUNCTION = "create temporary function ";
    private static final SetProcessor setProcessor = new SetProcessor();
    static Logger LOG = LoggerFactory.getLogger("LineageInfo");

    private static final LineageLogger lineageLogger = new LineageLogger();

    public static void main(String[] args) throws Exception {
        HiveConf conf = new HiveConf();
        conf.addResource(new Path("file:///opt/module/hive/conf/hive-site.xml"));
        conf.set("hive.exec.post.hooks", "org.apache.hadoop.hive.ql.hooks.LineageLogger");


//        String command = args[0];
        String command = " select  user_id,collect_set(named_struct('sku_id',sku_id,'sku_num',sku_num,'order_count',order_count,'order_amount',order_amount)) order_stats from   (select     user_id,sku_id,sum(sku_num) sku_num,count(*) order_count,cast(sum(final_amount_d) as decimal(20,2)) order_amount from gmall.dwd_fact_order_detail where dt='2021-04-14' group by user_id,sku_id)tmp group by user_id";


        SessionState.start(conf);

        QueryState queryState = new QueryState.Builder().withGenerateNewQueryId(true).withHiveConf(conf).build();

        String queryId = queryState.getQueryId();
        Context ctx = new Context(conf);


        System.out.println("开始解析----------------");
        ASTNode tree;
        try {
            tree = ParseUtils.parse(command, ctx);
        } catch (ParseException e) {
            System.out.println("parse error");
            throw e;
        } finally {

        }
        LineageState parseLineage = queryState.getLineageState();


        String flag = parseLineage.getIndex().getFinalSelectOps().size() > 0 ? "是" : "否";
        System.out.println("解析后是否有血缘：" + flag);


        BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(queryState, tree);

        System.out.println("开始编译+优化------------------");
        sem.analyze(tree, ctx);
        LineageState lineageState = queryState.getLineageState();
        flag = lineageState.getIndex().getFinalSelectOps().size() > 0 ? "是" : "否";
        System.out.println("编译+优化后是否有血缘：" + flag);

        // validate the plan
//        sem.validate();

        Schema schema;
        schema = getSchema(sem, conf);
        QueryPlan plan = new QueryPlan(command, sem, 0L, queryId,
                queryState.getHiveOperation(), schema);


        //execute

//        String guid64 = Base64.encodeBase64URLSafeString(foo().getGuid()).trim();
        LineageCtx.Index index = queryState.getLineageState().getIndex();

        StringBuilderWriter out = new StringBuilderWriter(1024);
        JsonWriter writer = new JsonWriter(out);
        writer.beginObject();
        List<LineageLogger.Edge> edges = LineageLogger.getEdges(plan, index);
        Set<LineageLogger.Vertex> vertices = LineageLogger.getVertices(edges);
        LineageLogger.writeEdges(writer, edges);
        LineageLogger.writeVertices(writer, vertices);
        writer.endObject();
        writer.close();

        // Logger the lineage info
        String lineage = out.toString();

        System.out.println(lineage + " ------hello world");

        System.exit(0);


    }

    public static Schema getSchema(BaseSemanticAnalyzer sem, HiveConf conf) {
        Schema schema = null;


        if (sem == null) {
            // can't get any info without a plan
        } else if (sem.getResultSchema() != null) {
            List<FieldSchema> lst = sem.getResultSchema();
            schema = new Schema(lst, null);
        } else if (sem.getFetchTask() != null) {
            FetchTask ft = sem.getFetchTask();
            TableDesc td = ft.getTblDesc();

            if (td == null && ft.getWork() != null && ft.getWork().getPartDesc() != null) {
                if (ft.getWork().getPartDesc().size() > 0) {
                    td = ft.getWork().getPartDesc().get(0).getTableDesc();
                }
            }

            if (td == null) {
                LOG.info("No returning schema.");
            } else {
                String tableName = "result";
                List<FieldSchema> lst = null;
                try {
                    lst = HiveMetaStoreUtils.getFieldsFromDeserializer(tableName, td.getDeserializer(conf));
                } catch (Exception e) {
                    LOG.warn("Error getting schema: "
                            + org.apache.hadoop.util.StringUtils.stringifyException(e));
                }
                if (lst != null) {
                    schema = new Schema(lst, null);
                }
            }
        }
        if (schema == null) {
            schema = new Schema();
        }
        LOG.info("Returning Hive schema: " + schema);
        return schema;
    }

}