package com.losttemple.sql.language.operator

import com.losttemple.sql.language.dialects.JdbcSqlSegment
import com.losttemple.sql.language.generate.*
import com.losttemple.sql.language.types.*
import java.sql.Connection
import java.sql.ResultSet
import java.time.Duration
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap

interface SqlDialect {
    fun table(name: String)
    fun where()
    fun having()
    fun column(name: String)
    fun column(table: String, name: String)
    fun constance(value: Int?)
    fun constance(value: String?)
    fun constance(value: Date?)
    fun constance(value: Boolean?)
    fun constance(value: Double?)
    fun columnList()
    fun addToList()
    fun select()
    fun rename(newName: String)
    fun and()
    fun or()
    fun eq()
    fun greater()
    fun add()
    fun subtraction()
    fun addPeriod(period: Duration)
    fun subPeriod(period: Duration)
    fun leftJoin()
    fun rightJoin()
    fun innerJoin()
    fun outerJoin()
    fun max()
    fun min()
    fun sum()
    fun count()
    fun now()
    fun insert(name: String)
    fun insertWithColumns(name: String)
    fun assign()
    fun assignList()
    fun addAssign()
    fun updateWithFilter()
    fun updateAll()
    fun delete()
    fun deleteAll(name: String)
    fun order()
    fun orderDesc()
    fun limit(count: Int)
    fun group()
    val sql: JdbcSqlSegment

    fun intResult(result: ResultSet, name: String): Int?
    fun stringResult(result: ResultSet, name: String): String?
    fun dateResult(result: ResultSet, name: String): Date?
    fun boolResult(result: ResultSet, name: String): Boolean?
    fun doubleResult(result: ResultSet, name: String): Double?
}

class QueryResultAccessor(private val dialect: SqlDialect,
                          private val resultSet: ResultSet, private val mapping: Map<SqlTypeCommon, String>) {
    fun <T> get(value: SqlType<T>): T? {
        val name = mapping[value] ?: error("")
        return value.getFromResult(dialect, resultSet, name)
    }

    val <T> SqlType<T>.v: T?
        get() {
            val name = mapping[this] ?: error("")
            return this.getFromResult(dialect, resultSet, name)
        }

    operator fun <T> SqlType<T>.invoke(): T? {
        val name = mapping[this] ?: error("")
        return this.getFromResult(dialect, resultSet, name)
    }
}

class DbResult<T>(private val columns: List<SqlTypeCommon>, private val description: T, private val source: SqlSet) {
    fun <R> select(machine: SqlDialect, connection: Connection, mapper: QueryResultAccessor.(T)->R): List<R> {
        val query = DefaultQueryConstructor()
        source.push(query)
        val pushed = ArrayList<ContractExpression>()
        for (column in columns) {
            val selector = query.addSelector()
            val c = selector.contract()
            column.push(c)
            pushed.add(c)
        }
        for (i in pushed) {
            i.commit()
        }
        val context = EvaluateContext(machine, CountIdGenerator(), mapOf(), mapOf())
        val mapping = query.root(context)
        val columnMapping = HashMap<SqlTypeCommon, String>()
        for (index in columns.indices) {
            columnMapping[columns[index]] = mapping[index]
        }
        println(machine.sql.describe())
        return machine.sql.prepare(connection).use {
            statement ->
            val result = statement.executeQuery()
                result.use {
                val resultList = ArrayList<R>()
                val accessor = QueryResultAccessor(machine, it, columnMapping)
                while (it.next()) {
                    val r = accessor.mapper(description)
                    resultList.add(r)
                }
                resultList
            }
        }
    }

    /*fun sql(machine: SqlDialect): String {
        val generator = SqlGenerator(machine).withoutSource(SourcePath(""))
        generator.root(columns, source)
        return machine.sql.describe()
    }*/
}

fun <T> DbSet<T>.use(config: CalcConfig.(T)->Unit): DbResult<T> {
    val configStorage = CalcConfig(set)
    configStorage.config(description)
    return configStorage.generate(description)
}

class DbInstanceResult<T>(private val columns: List<SqlTypeCommon>, private val description: T, private val source: SqlSet) {
    fun <R> select(machine: SqlDialect, connection: Connection, mapper: QueryResultAccessor.(T)->R): List<R> {
        val query = DefaultQueryConstructor()
        source.push(query)
        val pushed = ArrayList<ContractExpression>()
        for (column in columns) {
            val selector = query.addSelector()
            val c = selector.contract()
            column.push(c)
            pushed.add(c)
        }
        for (i in pushed) {
            i.commit()
        }
        val context = EvaluateContext(machine, CountIdGenerator(), mapOf(), mapOf())
        val mapping = query.root(context)
        val columnMapping = HashMap<SqlTypeCommon, String>()
        for (index in columns.indices) {
            columnMapping[columns[index]] = mapping[index]
        }
        println(machine.sql.describe())
        return machine.sql.prepare(connection).use {
            statement ->
            val result = statement.executeQuery()
            result.use {
                val resultList = ArrayList<R>()
                val accessor = QueryResultAccessor(machine, it, columnMapping)
                while (it.next()) {
                    val r = accessor.mapper(description)
                    resultList.add(r)
                }
                resultList
            }
        }
    }
/*
    fun sql(machine: SqlDialect): String {
        val generator = SqlGenerator(machine).withoutSource(SourcePath(""))
        generator.root(columns, source)
        return machine.sql.describe()
    }
 */
}

fun <T> DbInstance<T>.use(config: CalcConfig.(T)->Unit): DbInstanceResult<T> {
    val configStorage = CalcConfig(set)
    configStorage.config(description)
    return configStorage.generateInstance(description)
}

operator fun <T> DbInstance<SqlType<T>>.invoke(machine: SqlDialect, connection: Connection): T? {
    val query = DefaultQueryConstructor()
    set.push(query)
    val selector = query.addSelector()
    value.push(selector)
    val context = EvaluateContext(machine, CountIdGenerator(), mapOf(), mapOf())
    val mapping = query.root(context)
    val name = mapping.first()
    println(machine.sql.describe())
    return machine.sql.prepare(connection).use {
        statement ->
        val result = statement.executeQuery()
        result.use {
            result.next()
            value.getFromResult(machine, it, name)
        }
    }
}