package com.losttemple.sql.language.dialects

import com.losttemple.sql.language.operator.*
import com.losttemple.sql.language.types.SqlType
import com.losttemple.sql.language.wrappers.ConnectionEnvironment
import java.math.BigInteger
import java.sql.*
import java.time.Duration
import java.util.*
import java.util.Date

data class JdbcSqlserverSegment(val sql: String, val limit: Int, val parameters: List<JdbcSqlParameter>) {
    fun prepare(connection: Connection): PreparedStatement {
        val statement = connection.prepareStatement(sql);
        for ((index, parameter) in parameters.withIndex()) {
            parameter.setParam(statement, index + 1)
        }
        return statement
    }

    infix operator fun plus(another: JdbcSqlSegment): JdbcSqlSegment {
        val mergedSql = sql + another.sql
        val mergedParameters = parameters + another.parameters
        return JdbcSqlSegment(mergedSql, mergedParameters)
    }

    infix operator fun plus(another: String): JdbcSqlSegment {
        val mergedSql = sql + another
        return JdbcSqlSegment(mergedSql, parameters)
    }

    fun describe(): String {
        return "$sql\n${parameters.map { it.toString() }.joinToString(separator = "\n")}"
    }

    override fun toString(): String {
        return sql
    }
}

class SqlServerDialect: SqlDialect {
    private val stack = Stack<JdbcSqlserverSegment>()

    private fun pop(): JdbcSqlserverSegment {
        return stack.pop()
    }

    private fun push(sql: String, limit: Int, parameters: List<JdbcSqlParameter>) {
        val segment = JdbcSqlserverSegment(sql, limit, parameters)
        stack.push(segment)
    }

    private fun push(sql: String) {
        val segment = JdbcSqlserverSegment(sql, 0, emptyList())
        stack.push(segment)
    }

    private fun push(statement: JdbcSqlserverSegment) {
        stack.push(statement)
    }

    private fun push(sql: String, limit: Int, parameter: JdbcSqlParameter) {
        val segment = JdbcSqlserverSegment(sql, limit, listOf(parameter))
        stack.push(segment)
    }

    private fun push(sql: String, vararg segments: JdbcSqlserverSegment) {
        val count = segments.sumBy { it.parameters.size }
        val result = ArrayList<JdbcSqlParameter>(count)
        for (segment in segments) {
            result.addAll(segment.parameters)
        }
        push(sql, segments.sumBy { it.limit }, result)
    }

    override fun table(name: String) {
        push("[${name}]")
    }

    override fun where() {
        val condition = pop()
        val fromSource = pop()
        push("$fromSource WHERE $condition", fromSource, condition)
    }

    override fun having() {
        val condition = pop()
        val fromSource = pop()
        push("$fromSource HAVING $condition", fromSource, condition)
    }

    override fun column(name: String) {
        push("[$name]")
    }

    override fun column(table: String, name: String) {
        push("[$table].[$name]")
    }

    override fun constance(value: BigInteger?) {
        push("?", 0, JdbcIntParameter(value))
    }

    override fun constance(value: String?) {
        push("?", 0, JdbcStringParameter(value))
    }

    override fun constance(value: java.sql.Date?) {
        push("?", 0, JdbcDateParameter(value))
    }

    override fun constance(value: Time?) {
        push("?", 0, JdbcTimeParameter(value))
    }

    override fun constance(value: Timestamp?) {
        push("?", 0, JdbcTimestampParameter(value))
    }

    override fun constance(value: Boolean?) {
        push("?", 0, JdbcBoolAsIntParameter(value))
    }

    override fun constance(value: Double?) {
        push("?", 0, JdbcDoubleParameter(value))
    }

    override fun columnList() {
    }

    override fun addToList() {
        val newColumn = pop()
        val columnList = pop()
        push("$columnList, $newColumn", columnList, newColumn)
    }

    override fun select() {
        val columns = pop()
        val sourceSet = pop()
        if (sourceSet.limit > 0) {
            if (stack.size > 0) {
                push("(SELECT TOP ${sourceSet.limit} $columns FROM $sourceSet)", columns, sourceSet)
            } else {
                push("SELECT TOP ${sourceSet.limit} $columns FROM $sourceSet", columns, sourceSet)
            }
        }
        else {
            if (stack.size > 0) {
                push("(SELECT $columns FROM $sourceSet)", columns, sourceSet)
            } else {
                push("SELECT $columns FROM $sourceSet", columns, sourceSet)
            }
        }
    }

    override fun rename(newName: String) {
        val value = pop()
        push("$value AS $newName", value)
    }

    override fun and() {
        val right = pop()
        val left = pop()
        push("$left AND $right", left, right)
    }

    override fun or() {
        val right = pop()
        val left = pop()
        push("$left OR $right", left, right)
    }

    override fun eq() {
        val right = pop()
        val left = pop()
        push("$left = $right", left, right)
    }

    override fun greater() {
        val right = pop()
        val left = pop()
        push("$left > $right", left, right)
    }

    override fun add() {
        val right = pop()
        val left = pop()
        push("$left + $right", left, right)
    }

    override fun subtraction() {
        val right = pop()
        val left = pop()
        push("$left - $right", left, right)
    }

    private interface DurationPart {
        fun getPart(duration: Duration): Long
        fun fromPart(count: Long): Duration
        val name: String
    }

    private fun DurationPart.isWhole(duration: Duration): Boolean {
        val count = getPart(duration)
        val asWhole = fromPart(count)
        return duration == asWhole
    }

    companion object {
        private val defaultPart = object: DurationPart {
            override fun getPart(duration: Duration): Long {
                return duration.toNanos()
            }
            override fun fromPart(count: Long): Duration {
                return Duration.ofNanos(count)
            }
            override val name: String
                get() = "nanosecond"

        }
        private val durationParts = listOf<DurationPart>(
                object: DurationPart {
                    override fun getPart(duration: Duration): Long {
                        return duration.toDays()
                    }
                    override fun fromPart(count: Long): Duration {
                        return Duration.ofDays(count)
                    }
                    override val name: String
                        get() = "day"

                },
                object: DurationPart {
                    override fun getPart(duration: Duration): Long {
                        return duration.toHours()
                    }
                    override fun fromPart(count: Long): Duration {
                        return Duration.ofHours(count)
                    }
                    override val name: String
                        get() = "hour"
                },
                object: DurationPart {
                    override fun getPart(duration: Duration): Long {
                        return duration.toMinutes()
                    }
                    override fun fromPart(count: Long): Duration {
                        return Duration.ofMinutes(count)
                    }
                    override val name: String
                        get() = "minute"
                },
                object: DurationPart {
                    override fun getPart(duration: Duration): Long {
                        return duration.seconds
                    }
                    override fun fromPart(count: Long): Duration {
                        return Duration.ofSeconds(count)
                    }
                    override val name: String
                        get() = "second"
                },
                object: DurationPart {
                    override fun getPart(duration: Duration): Long {
                        return duration.toMillis()
                    }
                    override fun fromPart(count: Long): Duration {
                        return Duration.ofMillis(count)
                    }
                    override val name: String
                        get() = "microsecond"
                }
        )
    }

    override fun addPeriod(period: Duration) {
        val base = pop()
        if (period == Duration.ZERO) {
            return
        }
        if (period < Duration.ZERO) {
            return subPeriod(period.negated())
        }
        val wholePart = durationParts.firstOrNull{
            it.isWhole(period)
        }?: defaultPart
        push("DATEADD(${wholePart.name}, ${wholePart.getPart(period)}, $base)", base)
    }

    override fun subPeriod(period: Duration) {
        val base = pop()
        if (period == Duration.ZERO) {
            return
        }
        if (period < Duration.ZERO) {
            return subPeriod(period.negated())
        }
        val wholePart = durationParts.firstOrNull{
            it.isWhole(period)
        }?: defaultPart
        push("DATEADD(${wholePart.name}, -${wholePart.getPart(period)}, $base)", base)
    }

    override fun leftJoin() {
        val condition = pop()
        val right = pop()
        val left = pop()
        push("$left LEFT JOIN $right ON $condition", left, right, condition)
    }

    override fun rightJoin() {
        val condition = pop()
        val right = pop()
        val left = pop()
        push("$left RIGHT JOIN $right ON $condition", left, right, condition)
    }

    override fun innerJoin() {
        val condition = pop()
        val right = pop()
        val left = pop()
        push("$left INNER JOIN $right ON $condition", left, right, condition)
    }

    override fun outerJoin() {
        val condition = pop()
        val right = pop()
        val left = pop()
        push("$left FULL OUTER JOIN $right ON $condition")
    }

    override fun max() {
        val value = pop()
        push("MAX($value)", value)
    }

    override fun min() {
        val value = pop()
        push("MIN($value)", value)
    }

    override fun sum() {
        val value = pop()
        push("SUM($value)", value)
    }

    override fun count() {
        val value = pop()
        push("COUNT($value)", value)
    }

    override fun now() {
        push("GETDATE()")
    }

    override fun insert(name: String) {
        val values = pop()
        push("INSERT INTO [$name] VALUES ($values)", values)
    }

    override fun insertWithColumns(name: String) {
        val values = pop()
        val columns = pop()
        push("INSERT INTO [$name] ($columns) VALUES ($values)", values)
    }

    override fun assign() {
        val right = pop()
        val left = pop()
        push("$left = $right", left, right)
    }

    override fun assignList() {
    }

    override fun addAssign() {
        val newAssign = pop()
        val assignList = pop()
        push("$assignList, $newAssign", assignList, newAssign)
    }

    override fun updateWithFilter() {
        val filter = pop()
        val assigns = pop()
        val table = pop()
        push("UPDATE $table SET $assigns WHERE $filter", table, assigns, filter)
    }

    override fun updateAll() {
        val assigns = pop()
        val table = pop()
        push("UPDATE $table SET $assigns", table, assigns)
    }

    override fun delete() {
        val filter = pop()
        val table = pop()
        push("DELETE FROM $table WHERE $filter", table, filter)
    }

    override fun deleteAll(name: String) {
        push("DELETE FROM $name")
    }

    override fun order() {
        val key = pop()
        val source = pop()
        push("$source ORDER BY $key", source, key)
    }

    override fun orderDesc() {
        val key = pop()
        val source = pop()
        push("$source ORDER BY $key DESC", source, key)
    }

    override fun limitWithOffset(count: Int, offset: Int) {
        val source = pop()
        push("$source LIMIT $count OFFSET $offset", source)
    }

    override fun limit(count: Int) {
        val source = pop()
        push(source.sql, count, source.parameters)
    }

    override fun group() {
        val key = pop()
        val source = pop()
        push("$source GROUP BY $key", source, key)
    }

    override val sql: JdbcSqlSegment
        get() = stack.first().run { JdbcSqlSegment(sql, parameters) }

    override fun byteResult(result: ResultSet, name: String): Byte? {
        val value = result.getByte(name)
        val isNull = result.wasNull()
        if (isNull) {
            return null
        }
        return value
    }

    override fun shortResult(result: ResultSet, name: String): Short? {
        val value = result.getShort(name)
        val isNull = result.wasNull()
        if (isNull) {
            return null
        }
        return value
    }

    override fun intResult(result: ResultSet, name: String): Int? {
        val value = result.getInt(name)
        val isNull = result.wasNull()
        if (isNull) {
            return null
        }
        return value
    }
    override fun longResult(result: ResultSet, name: String): Long? {
        val value = result.getLong(name)
        val isNull = result.wasNull()
        if (isNull) {
            return null
        }
        return value
    }

    override fun bigIntResult(result: ResultSet, name: String): BigInteger? {
        val value = result.getBigDecimal(name).toBigInteger()
        val isNull = result.wasNull()
        if (isNull) {
            return null
        }
        return value
    }

    override fun stringResult(result: ResultSet, name: String): String? {
        val value = result.getString(name)
        val isNull = result.wasNull()
        if (isNull) {
            return null
        }
        return value
    }

    override fun dateResult(result: ResultSet, name: String): Date? {
        val value = result.getDate(name)
        val isNull = result.wasNull()
        if (isNull) {
            return null
        }
        return value
    }

    override fun boolResult(result: ResultSet, name: String): Boolean? {
        val intResult = result.getInt(name)
        val isNull = result.wasNull()
        if (isNull) {
            return null
        }
        return intResult != 0
    }

    override fun doubleResult(result: ResultSet, name: String): Double? {
        val value = result.getDouble(name)
        val isNull = result.wasNull()
        if (isNull) {
            return null
        }
        return value
    }
}

class SqlServerEnvironment(connectionString: String, user: String, password: String):
        GenericDialectEnvironment(
                CreateConnectionFactory("com.microsoft.sqlserver.jdbc.SQLServerDriver", connectionString, user, password),
                {SqlServerDialect()})

fun connectSqlServer(connection: String, user: String, password: String, accessor: SqlServerEnvironment.()->Unit) {
    SqlServerEnvironment(connection, user, password).use {
        it.accessor()
    }
}