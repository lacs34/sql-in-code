package com.losttemple.sql.language.operator.sources

import com.losttemple.sql.language.dialects.JdbcStringParameter
import com.losttemple.sql.language.generate.ExpressionConstructor
import com.losttemple.sql.language.operator.SqlDialect
import com.losttemple.sql.language.operator.SqlParameter
import com.losttemple.sql.language.operator.StringSqlParameter
import com.losttemple.sql.language.types.SetRef
import java.sql.ResultSet
import java.sql.Types
import java.util.*

class StringColumn(setReference: SetRef,
                   name: String,
                   val type: Int):
        CommonColumn<String, StringColumn>(setReference, name) {
    private var isCaseSensitive: Boolean = true
    private var charCount: Int = 20

    fun caseInsensitive(): StringColumn {
        isCaseSensitive = false
        return this
    }

    fun length(len: Int): StringColumn {
        charCount = len
        return this
    }

    override val typeDescription: SqlTypeDescription
        get() {
            return SqlTypeDescription(
                    type,
                    isCaseSensitive = isCaseSensitive,
                    precision = charCount,
                    isNullable = isNullable,
                    isSearchable = isSearchable)
        }

    override fun generateParameter(parameter: String?): SqlParameter {
        return StringSqlParameter(parameter)
    }

    override fun getFromResult(dialect: SqlDialect, result: ResultSet, name: String): String? {
        return dialect.stringResult(result, name)
    }
}