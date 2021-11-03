package com.vincer;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassPath com.vincer.LoadSql
 * @Description TODO
 * @Date 2021/11/4 1:56
 * @Created by Vincer
 **/
public class LoadSql {
	static List<String> loadSql(String sqlFile) {
		//存储单个文件中的sql语句列表
		List<String> sqlList = new ArrayList<String>();
		try {
			InputStream sqlFileIn = new FileInputStream(sqlFile);
			StringBuilder sqlSb = new StringBuilder();
			byte[] buff = new byte[1024];
			int byteRead = 0;
			while ((byteRead = sqlFileIn.read(buff)) != -1) {
				String addStr = new String(buff, 0, byteRead);
				if (!addStr.trim().isEmpty()) {
					sqlSb.append(new String(buff, 0, byteRead));
				}
			}

			// Windows 下换行是 \r\n, Linux 下是 \n 切分成多个sql语句
			String[] sqlArr = sqlSb.toString().split(";");
			for (String s : sqlArr) {
				String sql = s.trim().replaceAll("--.*", "").trim();
				String sqlup = sql.toUpperCase();
				if (!sqlup.isEmpty()&&!sqlup.startsWith("SET ") && !sqlup.startsWith("ALTER ") && !sqlup.startsWith("USE ") && !sqlup.startsWith("MSCK ") && !sqlup.endsWith(".JAR'") && !sqlup.startsWith("ADD ") && !sqlup.startsWith("USING ") && !sql.equals(";") && !sqlup.startsWith("DROP ") && !sql.startsWith("--")) {
						sqlList.add(sql.replaceAll("((\r\n)|\n)[\\s\t ]*(\\1)+", "$1"));
				}
			}
		} catch (Exception ex) {
			System.out.println("获取sql语句失败，文件：" + sqlFile + "  失败信息：" + ex.getMessage());
		}
		return sqlList;
	}
}
