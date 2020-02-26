package com.losttemple.sql.language.types

import com.losttemple.sql.language.operator.SqlDialect
import java.sql.ResultSet

interface SqlString: SqlType<String> {
    override fun getFromResult(dialect: SqlDialect, result: ResultSet, name: String): String? {
        return dialect.stringResult(result, name)
    }
}