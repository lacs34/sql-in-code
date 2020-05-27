package com.losttemple.sql.language.dialects

import java.sql.*
import java.util.*
import java.util.Date

interface JdbcSqlParameter {
    fun setParam(jdbc: PreparedStatement, index: Int)
}

class JdbcIntParameter(private val value: Int?): JdbcSqlParameter {
    override fun setParam(jdbc: PreparedStatement, index: Int) {
        if (value == null) {
            jdbc.setNull(index, Types.INTEGER)
        }
        else {
            jdbc.setInt(index, value)
        }
    }

    override fun toString(): String {
        return "Int: $value"
    }
}

class JdbcStringParameter(private val value: String?): JdbcSqlParameter {
    override fun setParam(jdbc: PreparedStatement, index: Int) {
        if (value == null) {
            jdbc.setNull(index, Types.VARCHAR)
        }
        else {
            jdbc.setString(index, value)
        }
    }

    override fun toString(): String {
        return "String: $value"
    }
}

class JdbcTimeParameter(private val value: Date?): JdbcSqlParameter {
    override fun setParam(jdbc: PreparedStatement, index: Int) {
        if (value == null) {
            jdbc.setNull(index, Types.TIME)
        }
        else {
            jdbc.setTimestamp(index, Timestamp(value.time))
        }
    }

    override fun toString(): String {
        return "Time: $value"
    }
}

class JdbcBoolAsIntParameter(private val value: Boolean?): JdbcSqlParameter {
    override fun setParam(jdbc: PreparedStatement, index: Int) {
        if (value == null) {
            jdbc.setNull(index, Types.INTEGER)
        }
        else {
            val intValue = if (value) 1 else 0
            jdbc.setInt(index, intValue)
        }
    }

    override fun toString(): String {
        return "Bool: $value"
    }
}

class JdbcDoubleParameter(private val value: Double?): JdbcSqlParameter {
    override fun setParam(jdbc: PreparedStatement, index: Int) {
        if (value == null) {
            jdbc.setNull(index, Types.DOUBLE)
        }
        else {
            jdbc.setDouble(index, value)
        }
    }

    override fun toString(): String {
        return "Double: $value"
    }
}

data class JdbcSqlSegment(val sql: String, val parameters: List<JdbcSqlParameter>) {
    fun prepare(connection: Connection): PreparedStatement {
        val statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
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

infix operator fun String.plus(another: JdbcSqlSegment): JdbcSqlSegment {
    val mergedSql = this + another.sql
    return JdbcSqlSegment(mergedSql, another.parameters)
}

fun connectTo(driver: String, connection: String, user: String, password: String): Connection {
    Class.forName(driver)
    return DriverManager.getConnection(connection, user, password)
}

fun connectTo(driver: String, connection: String, user: String): Connection {
    Class.forName(driver)
    return DriverManager.getConnection(connection, user, null)
}

fun connectTo(driver: String, connection: String): Connection {
    Class.forName(driver)
    return DriverManager.getConnection(connection, null, null)
}