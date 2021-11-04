package com.vincer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.vincer.HiveSqlLineage.*;

/**
 * @ClassPath com.vincer.LineageApp
 * @Description TODO
 * @Date 2021/11/4 9:55
 * @Created by Vincer
 **/
public class LineageApp {
	//进行测试
	public static void main(String[] args) {
		String sqlFile="src/main/resources/test.sql";
		String taskName="testTask";
		List<String> sqls = LoadSql.loadSqls(sqlFile);
		ArrayList<HashMap> tableLineages = getTableLineages(sqls, taskName, sqlFile);
		System.out.println(tableLineages);


		String sql="Select Sno\n" +
				"from sc\n" +
				"Where sc.Grade<60\n" +
				"Group by Sno\n" +
				"Having count(Cno)>=2";
		System.out.println(rTrim(sql));

	}
}
