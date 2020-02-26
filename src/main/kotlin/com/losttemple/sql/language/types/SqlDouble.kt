package com.losttemple.sql.language.types

import com.losttemple.sql.language.operator.SqlDialect
import java.sql.ResultSet
import java.util.*

interface SqlDouble: SqlType<Double> {
    override fun getFromResult(dialect: SqlDialect, result: ResultSet, name: String): Double? {
        return dialect.doubleResult(result, name)
    }
}