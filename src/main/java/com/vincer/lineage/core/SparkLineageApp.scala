package com.vincer.lineage.core

import com.vincer.lineage.core.spark.SparkSqlLineage
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlParser

/**
	* @ClassPath com.vincer.lineage.core.SparkLineageApp
	* @Description TODO
	* @Date 2021/11/7 12:40
	* @Created by Vincer
	* */
object SparkLineageApp {
	def main(args: Array[String]): Unit = {
		val parser = new SparkSqlParser()
		val p = new SparkSqlLineage
		val sql: String = getSql
		val logicalPlan: LogicalPlan = parser.parsePlan(sql)
		println(p.recursionParsePlan(logicalPlan))
	}

	def getSql() = {
		"""
			|insert overwrite table app_bi.edu_income_tableau_report_month partition (by_month)
			|SELECT
			|    substring(date_sub(CURRENT_DATE, 1),1,7) as month,
			|    sum(final_price) as month_total_pay_fee,--本月总业绩
			|    sum(case when a.first_order_dt!=a.dt  then final_price else null end) as month_again_pay_fee,--本月续费业绩
			|    sum(case when a.first_order_dt=a.dt then a.final_price else null end) as month_first_pay_fee,--本月新签业绩
			|    sum(case when a.first_order_dt=a.dt AND a.source_name not LIKE '%8销售%' AND b.id is not null then a.final_price else null end) as month_new_first_pay_fee,--本月新签-新线索业绩
			|    sum(case when a.first_order_dt=a.dt AND a.source_name not LIKE '%8销售%' AND b.id is null then a.final_price else null end) as month_old_first_pay_fee, --本月新签-非线索业绩
			|    sum(case when a.first_order_dt=a.dt AND a.source_name LIKE '%8销售%'  then a.final_price else null end) as month_referral_pay_fee,--本月转介绍业绩
			|    substring(date_sub(CURRENT_DATE, 1),1,7) as by_month
			|    FROM
			|    (SELECT
			|    order_id,
			|    customer_id,
			|    source_name,
			|    final_price,
			|    first_order_dt,
			|    dt
			|    FROM fdm.fdm_m_edu_user_orders_day
			|    WHERE dt BETWEEN date_sub(CURRENT_DATE, 32) and date_sub(CURRENT_DATE, 1)
			|    AND substring(dt,1,7)=substring(date_sub(CURRENT_DATE, 1),1,7)
			|    AND source_name not like '%低价课%'
			|    AND source_name not like '%B端%'
			|    AND is_company=0
			|    group by
			|    order_id,
			|    customer_id,
			|    source_name,
			|    final_price,
			|    first_order_dt,
			|    dt) as a
			|
			|    left join
			|
			|    (select id
			|    from fdm.fdm_m_edu_user_crm_new_day
			|    where dt BETWEEN date_sub(CURRENT_DATE, 32) and date_sub(CURRENT_DATE, 1)
			|    AND substring(dt,1,7)=substring(date_sub(CURRENT_DATE, 1),1,7)
			|    and is_company=0
			|    group by id) as b
			|
			|    on a.customer_id=b.id
			|    group by substring(date_sub(CURRENT_DATE, 1),1,7)
			|""".stripMargin

	}
}
