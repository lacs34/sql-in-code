package com.losttemple.sql.language.types

import com.losttemple.sql.language.operator.SqlDialect
import java.sql.ResultSet

interface SqlBool: SqlType<Boolean> {
    override fun getFromResult(dialect: SqlDialect, result: ResultSet, name: String): Boolean? {
        return dialect.boolResult(result, name)
    }
}