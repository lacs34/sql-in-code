package com.losttemple.sql.language.types

import com.losttemple.sql.language.operator.SqlDialect
import java.sql.ResultSet

interface SqlInt: SqlType<Int> {
    override fun getFromResult(dialect: SqlDialect, result: ResultSet, name: String): Int? {
        return dialect.intResult(result, name)
    }
}