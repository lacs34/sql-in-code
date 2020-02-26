package com.losttemple.sql.language.types

import com.losttemple.sql.language.operator.SqlDialect
import java.sql.ResultSet
import java.util.*

interface SqlDate: SqlType<Date> {
    override fun getFromResult(dialect: SqlDialect, result: ResultSet, name: String): Date? {
        return dialect.dateResult(result, name)
    }
}