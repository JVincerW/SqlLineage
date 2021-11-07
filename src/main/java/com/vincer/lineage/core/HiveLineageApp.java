package com.vincer.lineage.core;

import com.vincer.lineage.core.common.LoadSql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.vincer.lineage.core.hive.HiveSqlLineage.*;

/**
 * @ClassPath com.vincer.LineageApp
 * @Description TODO
 * @Date 2021/11/4 9:55
 * @Created by Vincer
 **/
public class HiveLineageApp {
	//进行测试
	public static void main(String[] args) {
		String sqlFile="src/main/resources/test.sql";
		String taskName="testTask";
		List<String> sqls = LoadSql.loadSqls(sqlFile);
		ArrayList<HashMap> tableLineages = getTableLineages(sqls, taskName, sqlFile);
		System.out.println(tableLineages);
	}
}
