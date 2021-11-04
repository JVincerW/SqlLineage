package com.vincer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import static com.vincer.HiveSqlLineage.getTableLineages;

/**
 * @ClassPath com.vincer.LineageApp
 * @Description TODO
 * @Date 2021/11/4 9:55
 * @Created by Vincer
 **/
public class LineageApp {
	//进行测试
	public static void main(String[] args) throws Exception {
		String sqlFile="src/main/resources/test.sql";
		String taskName="testTask";
		List<String> sqls = LoadSql.loadSqls(sqlFile);
		ArrayList<HashMap> tableLineages = getTableLineages(sqls, taskName, sqlFile);
		System.out.println(tableLineages);
	}
}
