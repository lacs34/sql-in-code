package com.losttemple.sql.language.generate

import com.losttemple.sql.language.dialects.JdbcDateParameter
import com.losttemple.sql.language.dialects.JdbcSqlSegment
import com.losttemple.sql.language.dialects.JdbcTimeParameter
import com.losttemple.sql.language.dialects.JdbcTimestampParameter
import com.losttemple.sql.language.operator.SqlDialect
import java.math.BigInteger
import java.sql.*
import java.time.Duration
import java.util.*
import java.util.Date

interface HashSqlParameter {
    val parameterHash: String
}

class HashIntParameter(private val value: BigInteger?): HashSqlParameter {
    override val parameterHash: String
        get() = "Int: $value"
}

class HashStringParameter(private val value: String?): HashSqlParameter {
    override val parameterHash: String
        get() = "String: $value"
}

class HashTimeParameter(private val value: Date?): HashSqlParameter {
    override val parameterHash: String
        get() = "Date: $value"
}

class HashBoolParameter(private val value: Boolean?): HashSqlParameter {
    override val parameterHash: String
        get() = "Bool: $value"
}

class HashDoubleParameter(private val value: Double?): HashSqlParameter {
    override val parameterHash: String
        get() = "Double: $value"
}

data class HashSqlSegment(val sql: String, val parameters: List<HashSqlParameter>) {
    override fun equals(other: Any?): Boolean {
        val anotherSegment = other as? HashSqlSegment ?: return false
        if (sql != anotherSegment.sql) {
            return false
        }
        if (parameters.size != anotherSegment.parameters.size) {
            return false
        }
        for ((index, parameter) in parameters.withIndex()) {
            if (anotherSegment.parameters[index].parameterHash != parameter.parameterHash) {
                return false
            }
        }
        return true
    }

    override fun hashCode(): Int {
        return sql.hashCode()
    }

    infix operator fun plus(another: HashSqlSegment): HashSqlSegment {
        val mergedSql = sql + another.sql
        val mergedParameters = parameters + another.parameters
        return HashSqlSegment(mergedSql, mergedParameters)
    }

    infix operator fun plus(another: String): HashSqlSegment {
        val mergedSql = sql + another
        return HashSqlSegment(mergedSql, parameters)
    }

    fun describe(): String {
        return "$sql\n${parameters.map { it.toString() }.joinToString(separator = "\n")}"
    }

    override fun toString(): String {
        return sql
    }
}

class HashDialect: SqlDialect {
    private val stack = Stack<HashSqlSegment>()

    private fun pop(): HashSqlSegment {
        return stack.pop()
    }

    private fun push(sql: String, parameters: List<HashSqlParameter>) {
        val segment = HashSqlSegment(sql, parameters)
        stack.push(segment)
    }

    private fun push(sql: String) {
        val segment = HashSqlSegment(sql, emptyList())
        stack.push(segment)
    }

    private fun push(sql: String, parameter: HashSqlParameter) {
        val segment = HashSqlSegment(sql, listOf(parameter))
        stack.push(segment)
    }

    private fun push(statement: HashSqlSegment) {
        stack.push(statement)
    }

    private fun push(sql: String, vararg segments: HashSqlSegment) {
        val count = segments.sumBy { it.parameters.size }
        val result = ArrayList<HashSqlParameter>(count)
        for (segment in segments) {
            result.addAll(segment.parameters)
        }
        push(sql, result)
    }

    override fun table(name: String) {
        push("`${name}`")
    }

    override fun where() {
        val condition = pop()
        val fromSource = pop()
        push("$fromSource WHERE $condition", fromSource, condition)
    }

    override fun having() {
        val condition = pop()
        val fromSource = pop()
        push("$fromSource HEAVING $condition", fromSource, condition)
    }

    override fun column(name: String) {
        push("`$name`")
    }

    override fun column(table: String, name: String) {
        push("`$table`.`$name`")
    }

    override fun constance(value: BigInteger?) {
        push("?", HashIntParameter(value))
    }

    override fun constance(value: String?) {
        push("?", HashStringParameter(value))
    }

    override fun constance(value: java.sql.Date?) {
        push("?", HashTimeParameter(value))
    }

    override fun constance(value: Time?) {
        push("?", HashTimeParameter(value))
    }

    override fun constance(value: Timestamp?) {
        push("?", HashTimeParameter(value))
    }

    override fun constance(value: Boolean?) {
        push("?", HashBoolParameter(value))
    }

    override fun constance(value: Double?) {
        push("?", HashDoubleParameter(value))
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
        if (stack.size > 0) {
            push("(SELECT $columns FROM $sourceSet)", columns, sourceSet)
        }
        else {
            push("SELECT $columns FROM $sourceSet", columns, sourceSet)
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
                return duration.toMillis()
            }
            override fun fromPart(count: Long): Duration {
                return Duration.ofMillis(count)
            }
            override val name: String
                get() = "MICROSECOND"

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
                        get() = "DAY"

                },
                object: DurationPart {
                    override fun getPart(duration: Duration): Long {
                        return duration.toHours()
                    }
                    override fun fromPart(count: Long): Duration {
                        return Duration.ofHours(count)
                    }
                    override val name: String
                        get() = "HOUR"
                },
                object: DurationPart {
                    override fun getPart(duration: Duration): Long {
                        return duration.toMinutes()
                    }
                    override fun fromPart(count: Long): Duration {
                        return Duration.ofMinutes(count)
                    }
                    override val name: String
                        get() = "MINUTE"
                },
                object: DurationPart {
                    override fun getPart(duration: Duration): Long {
                        return duration.seconds
                    }
                    override fun fromPart(count: Long): Duration {
                        return Duration.ofSeconds(count)
                    }
                    override val name: String
                        get() = "SECOND"
                }
        )
    }

    private fun removeNanoSeconds(period: Duration): Duration {
        val milliseconds = period.toMillis()
        return Duration.ofMillis(milliseconds)
    }

    override fun addPeriod(period: Duration) {
        val base = pop()
        val duration = removeNanoSeconds(period)
        if (duration == Duration.ZERO) {
            return
        }
        if (duration < Duration.ZERO) {
            return subPeriod(duration.negated())
        }
        val firstPart = durationParts.firstOrNull{
            it.getPart(duration) > 0
        }?: defaultPart
        if (firstPart.isWhole(duration)) {
            push("DATE_ADD($base, INTERVAL ${firstPart.getPart(duration)} ${firstPart.name})", base)
        }
        else {
            val wholePart = durationParts.firstOrNull {
                it.isWhole(duration)
            } ?: defaultPart
            val firstPartValue = firstPart.getPart(duration)
            val remainder = duration.minus(firstPart.fromPart(firstPartValue))
            push("DATE_ADD($base, INTERVAL '${firstPartValue} ${wholePart.getPart(remainder)}' ${firstPart.name}_${wholePart.name})", base)
        }
    }

    override fun subPeriod(period: Duration) {
        val base = pop()
        val duration = removeNanoSeconds(period)
        if (duration == Duration.ZERO) {
            return
        }
        if (duration < Duration.ZERO) {
            return subPeriod(duration.negated())
        }
        val firstPart = durationParts.firstOrNull{
            it.getPart(duration) > 0
        }?: defaultPart
        if (firstPart.isWhole(duration)) {
            push("DATE_SUB($base, INTERVAL ${firstPart.getPart(duration)} ${firstPart.name})", base)
        }
        else {
            val wholePart = durationParts.firstOrNull {
                it.isWhole(duration)
            } ?: defaultPart
            val firstPartValue = firstPart.getPart(duration)
            val remainder = duration.minus(firstPart.fromPart(firstPartValue))
            push("DATE_SUB($base, INTERVAL '${firstPartValue} ${wholePart.getPart(remainder)}' ${firstPart.name}_${wholePart.name})", base)
        }
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
        push("NOW()")
    }

    override fun insert(name: String) {
        val values = pop()
        push("INSERT INTO `$name`", values)
    }

    override fun insertWithColumns(name: String) {
        val values = pop()
        val columns = pop()
        push("INSERT INTO `$name` ($columns) VALUES ($values)", values)
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

    override fun descKey() {
        val key = pop()
        push("$key DESC", key)
    }

    override fun limitWithOffset(count: Int, offset: Int) {
        val source = pop()
        push("$source LIMIT $count OFFSET $offset", source)
    }

    override fun limit(count: Int) {
        val source = pop()
        push("$source LIMIT $count", source)
    }

    override fun group() {
        val key = pop()
        val source = pop()
        push("$source GROUP BY $key", source, key)
    }

    override val sql: JdbcSqlSegment
        get() = error("")

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

    fun hash(): HashSqlSegment {
        return stack.first()
    }
}