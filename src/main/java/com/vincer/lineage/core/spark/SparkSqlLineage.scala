package com.vincer.lineage.core.spark

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.UnresolvedCatalogRelation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Deduplicate, DeserializeToObject, Distinct, Filter, Generate, GlobalLimit, GroupingSets, InsertIntoStatement, Join, LocalLimit, LogicalPlan, MapElements, MapPartitions, OneRowRelation, Project, Repartition, RepartitionByExpression, SerializeFromObject, Sort, SubqueryAlias, TypedFilter, Union, Window, With}
import org.apache.spark.sql.execution.command.{AddJarCommand, AlterDatabasePropertiesCommand, AlterTableAddColumnsCommand, AlterTableAddPartitionCommand, AlterTableDropPartitionCommand, AlterTableRecoverPartitionsCommand, CreateDatabaseCommand, CreateFunctionCommand, CreateTableLikeCommand, DropDatabaseCommand, DropFunctionCommand, DropTableCommand, SetCommand, ShowCreateTableCommand}
import org.apache.spark.sql.execution.datasources.CreateTable
import java.util.{HashSet => JSet}
/**
	* @ClassPath com.vincer.lineage.core.spark.SparkSqlLineage
	* @Description TODO
	* @Date 2021/11/7 12:38
	* @Created by Vincer
	* */
class SparkSqlLineage {
	def recursionParsePlan(plan: LogicalPlan): (JSet[String], JSet[String]) = {
		val inputs = new JSet[String]()
		val outputs = new JSet[String]()
		parsePlan(plan, inputs, outputs)
		Tuple2(inputs, outputs)
	}
	def parsePlan(plan: LogicalPlan, inputs: JSet[String], outputs: JSet[String]): Unit = {
		plan match {
			case plan: Project =>
				val project: Project = plan
				parsePlan(project.child, inputs, outputs)
			case plan: Join =>
				val project: Join = plan
				parsePlan(project.left, inputs, outputs)
				parsePlan(project.right, inputs, outputs)
			case plan: InsertIntoStatement =>
				val project: InsertIntoStatement = plan
				plan.table match {
					case relation: UnresolvedRelation =>
						val table = relation.tableName
						outputs.add(table)
					case _ =>
						throw new RuntimeException("无法解析:" + plan.table)
				}
				parsePlan(project.query, inputs, outputs)
			case plan: Aggregate =>
				val project: Aggregate = plan
				parsePlan(project.child, inputs, outputs)
			case plan: Filter =>
				val project: Filter = plan
				parsePlan(project.child, inputs, outputs)
			case plan: Generate =>
				val project: Generate = plan
				parsePlan(project.child, inputs, outputs)
			case plan: RepartitionByExpression =>
				val project: RepartitionByExpression = plan
				parsePlan(project.child, inputs, outputs)
			case plan: SerializeFromObject =>
				val project: SerializeFromObject = plan
				parsePlan(project.child, inputs, outputs)
			case plan: MapPartitions =>
				val project: MapPartitions = plan
				parsePlan(project.child, inputs, outputs)
			case plan: Sort =>
				val project: Sort = plan
				parsePlan(project.child, inputs, outputs)
			case ignore: SetCommand =>
				print(ignore.toString())
			case ignore: AddJarCommand =>
				print(ignore.toString())
			case ignore: CreateFunctionCommand =>
				print(ignore.toString())
			case ignore: OneRowRelation =>
				print(ignore.toString())
			case ignore: DropFunctionCommand =>
				print(ignore.toString())
			case plan: AlterTableAddPartitionCommand =>
				val project: AlterTableAddPartitionCommand = plan
				outputs.add(project.tableName.table)
			case plan: AlterTableDropPartitionCommand =>
				val project: AlterTableDropPartitionCommand = plan
				outputs.add(project.schemaString)
			case plan: Union =>
				val project: Union = plan
				for (child <- project.children) {
					parsePlan(child, inputs, outputs)
				}
			case plan: UnresolvedCatalogRelation =>
				val project: UnresolvedCatalogRelation = plan
				val identifier: TableIdentifier = project.tableMeta.identifier
				inputs.add(identifier.table)
			case plan: UnresolvedRelation =>
				val project: UnresolvedRelation = plan
				inputs.add(project.tableName)
			case plan: DropDatabaseCommand =>
				val project: DropDatabaseCommand = plan
				inputs.add(new String(project.databaseName))
			case plan: AlterDatabasePropertiesCommand =>
				val project: AlterDatabasePropertiesCommand = plan
				inputs.add(new String(project.databaseName))
			case plan: ShowCreateTableCommand =>
				val project: ShowCreateTableCommand = plan
				outputs.add(project.schemaString)
			case plan: DeserializeToObject =>
				val project: DeserializeToObject = plan
				parsePlan(project.child, inputs, outputs)
			case plan: AlterTableAddColumnsCommand =>
				val project: AlterTableAddColumnsCommand = plan
				outputs.add(project.schemaString)
			case plan: CreateTableLikeCommand =>
				val project: CreateTableLikeCommand = plan
				inputs.add(project.sourceTable.table)
				outputs.add(project.targetTable.table)
			case plan: DropTableCommand =>
				val project: DropTableCommand = plan
				outputs.add(project.schemaString)
			case plan: AlterTableRecoverPartitionsCommand =>
				val project: AlterTableRecoverPartitionsCommand = plan
				outputs.add(project.schemaString)
			case plan: GroupingSets =>
				val project: GroupingSets = plan
				parsePlan(project.child, inputs, outputs)
			case plan: CreateDatabaseCommand =>
				val project: CreateDatabaseCommand = plan
				inputs.add(new String(project.databaseName))
			case plan: Repartition =>
				val project: Repartition = plan
				parsePlan(project.child, inputs, outputs)
			case plan: Deduplicate =>
				val project: Deduplicate = plan
				parsePlan(project.child, inputs, outputs)
			case plan: Window =>
				val project: Window = plan
				parsePlan(project.child, inputs, outputs)
			case plan: MapElements =>
				val project: MapElements = plan
				parsePlan(project.child, inputs, outputs)
			case plan: TypedFilter =>
				val project: TypedFilter = plan
				parsePlan(project.child, inputs, outputs)
			case plan: Distinct =>
				val project: Distinct = plan
				parsePlan(project.child, inputs, outputs)
			case plan: SubqueryAlias =>
				val project: SubqueryAlias = plan
				val childinputs = new JSet[String]()
				val childoutputs = new JSet[String]()
				parsePlan(project.child, childinputs, childoutputs)
				if (childinputs.size() > 1) {
					childinputs.forEach(table => inputs.add(table))
				} else if (childinputs.size() == 1) {
					val ctb: String = childinputs.iterator().next()
					inputs.add(ctb)
				}
			case plan: CreateTable =>
				val project: CreateTable = plan
				if (project.query.isDefined) {
					parsePlan(project.query.get, inputs, outputs)
				}
				val tableIdentifier: TableIdentifier = project.tableDesc.identifier
				outputs.add(tableIdentifier.table)
			case plan: GlobalLimit =>
				val project: GlobalLimit = plan
				parsePlan(project.child, inputs, outputs)
			case plan: LocalLimit =>
				val project: LocalLimit = plan
				parsePlan(project.child, inputs, outputs)
			case plan: With =>
				val project: With = plan
				project.cteRelations.foreach((cte: (String, SubqueryAlias)) => {
					parsePlan(cte._2, inputs, outputs)
				})
				parsePlan(project.child, inputs, outputs)
			case plan => {
				throw new RuntimeException("plan:\n" + plan.getClass.getName + "\n" + plan)
			}
		}
	}
}
