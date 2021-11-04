package com.vincer;

import org.apache.hadoop.hive.ql.lib.*;
import org.apache.hadoop.hive.ql.parse.*;

import java.util.*;

/**
 * @ClassPath com.vincer.HiveLineageInfo
 * @Description TODO
 * @Date 2021/11/3 23:06
 * @Created by Vincer
 **/

public class HiveSqlLineage implements NodeProcessor {

	// 输入表
	TreeSet<String> inputTableList = new TreeSet<String>();

	// 存放目标表
	TreeSet<String> outputTableList = new TreeSet<String>();

	//存放with子句中的别名, 最终的输入表是 inputTableList减去withTableList
	TreeSet<String> withTableList = new TreeSet<String>();

	public TreeSet getInputTableList() {
		return inputTableList;
	}

	public TreeSet getOutputTableList() {
		return outputTableList;
	}

	public TreeSet getWithTableList() {
		return withTableList;
	}

	/*执行解析，对应值add到list*/
	public Object process(Node nd, Stack stack, NodeProcessorCtx procCtx, Object... nodeOutputs) {
		ASTNode pt = (ASTNode) nd;
		switch (pt.getToken().getType()) {
			//create语句
			case HiveParser.TOK_CREATETABLE:

				//insert语句
			case HiveParser.TOK_TAB: {
				String createName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) pt.getChild(0));
				outputTableList.add(createName);
				break;
			}

			//from语句
			case HiveParser.TOK_TABREF: {
				ASTNode tabTree = (ASTNode) pt.getChild(0);
				String fromName = (tabTree.getChildCount() == 1) ? BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) : BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) + "." + tabTree.getChild(1);
				inputTableList.add(fromName);
				break;
			}

			// with.....语句
			case HiveParser.TOK_CTE: {
				for (int i = 0; i < pt.getChildCount(); i++) {
					ASTNode temp = (ASTNode) pt.getChild(i);
					String cteName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) temp.getChild(1));
					withTableList.add(cteName);
				}
				break;
			}
		}
		return null;
	}

	/*执行解析对应语句部分*/
	public void getLineageInfo(String query) {

		ParseDriver pd = new ParseDriver();
		ASTNode tree;
		try {
			tree = pd.parse(query);
			while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
				tree = (ASTNode) tree.getChild(0);
			}
			inputTableList.clear();
			outputTableList.clear();
			withTableList.clear();
			Map<Rule, NodeProcessor> rules = new LinkedHashMap<Rule, NodeProcessor>();
			Dispatcher dftdsp = new DefaultRuleDispatcher(this, rules, null);
			GraphWalker ogw = new DefaultGraphWalker(dftdsp);
			ArrayList topNodes = new ArrayList();
			topNodes.add(tree);
			ogw.startWalking(topNodes, null);
		} catch (ParseException | SemanticException e) {
		}
	}

	/*解析单条sql*/
	public static Map<String, Object> ParseSql(String sql, String task, String sqlFile) {

		HiveSqlLineage lep = new HiveSqlLineage();
		lep.getLineageInfo(sql);
		Map<String, Object> map = new HashMap<>();
		map.put("inputs", lep.getInputTableList());
		map.put("outputs", lep.getOutputTableList());
		map.put("task", task);
		map.put("sqlFile", sqlFile);
		return map;
	}

	/*解析多条sql语句*/
	public static ArrayList<HashMap> getTableLineages(List<String> sqls, String task, String sqlFile) {
		ArrayList<HashMap> lineages = new ArrayList<>();
		for (String sql : sqls) {
			HiveSqlLineage lep = new HiveSqlLineage();
			HashMap resMap = (HashMap) ParseSql(sql, task, sqlFile);
			lineages.add(resMap);
		}
		return lineages;
	}
}