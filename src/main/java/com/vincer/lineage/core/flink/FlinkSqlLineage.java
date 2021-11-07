package com.vincer.lineage.core.flink;

import com.google.common.base.Joiner;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static org.apache.calcite.sql.SqlKind.*;

/**
 * @ClassPath com.vincer.lineage.core.flink.FlinkSqlLineage
 * @Description TODO
 * @Date 2021/11/7 16:53
 * @Created by Vincer
 **/
public class FlinkSqlLineage {
	static Pattern pattern = Pattern.compile("(create table ([\\w.]+) as )([\\s\\S]+)");
	public static void main(String[] args) throws SqlParseException {
		String test = "create table asf.afe as SELECT column_name ,adf FROM table1 UNION SELECT column_name FROM table2  UNION SELECT column_name FROM table2";
		List<String> dependencies = new LinkedList<>();
		// System.out.println(getSqlNode(test));
		System.out.println("=================================");
		getDependencies(getSqlNode(test), dependencies);
		System.out.println("dependencies: " + Joiner.on(",").join(dependencies));
	}
	public static SqlNode getSqlNode(String sql) throws SqlParseException {
		String formattedSql = sql.replaceAll(",", " , ").replaceAll("\\s+", " ").trim();
		if (formattedSql.toLowerCase().startsWith("create table ")) {
			System.out.println("sql creates table");
			Matcher m = pattern.matcher(formattedSql.toLowerCase());
			if (m.matches()) {
				System.out.println("matched: " + m.group(1).length());
				formattedSql = formattedSql.substring(m.group(1).length());
			} else {
				System.out.println("no matched");
			}
		}
		System.out.println(formattedSql);
		SqlNode sqlNode = SqlParser.create(formattedSql, SqlParser.Config.DEFAULT).parseQuery();
		System.out.println(sqlNode.getKind());
		return sqlNode;
	}
	public static List<String> getDependencies(SqlNode sqlNode, List<String> result) {
		if (sqlNode.getKind() == JOIN) {
			SqlJoin sqlKind = (SqlJoin) sqlNode;
			getDependencies(sqlKind.getLeft(), result);
			getDependencies(sqlKind.getRight(), result);
		}
		if (sqlNode.getKind() == IDENTIFIER) {
			result.add(sqlNode.toString());
		}
		if (sqlNode.getKind() == INSERT) {
			SqlInsert sqlKind = (SqlInsert) sqlNode;
			getDependencies(sqlKind.getSource(), result);
		}
		if (sqlNode.getKind() == SELECT) {
			SqlSelect sqlKind = (SqlSelect) sqlNode;
			assert sqlKind.getFrom() != null;
			getDependencies(sqlKind.getFrom(), result);
		}
		if (sqlNode.getKind() == AS) {
			assert sqlNode instanceof SqlBasicCall;
			SqlBasicCall sqlKind = (SqlBasicCall) sqlNode;
			getDependencies(sqlKind.getOperandList().get(0), result);
		}
		if (sqlNode.getKind() == UNION) {
			assert sqlNode instanceof SqlBasicCall;
			SqlBasicCall sqlKind = (SqlBasicCall) sqlNode;
			getDependencies(sqlKind.getOperandList().get(0), result);
			getDependencies(sqlKind.getOperandList().get(1), result);
		}
		if (sqlNode.getKind() == ORDER_BY) {
			assert sqlNode instanceof SqlOrderBy;
			SqlOrderBy sqlKind = (SqlOrderBy) sqlNode;
			getDependencies(sqlKind.getOperandList().get(0), result);
		}
		return result;
	}
}
