package com.losttemple.sql.language.operator.sources

import com.losttemple.sql.language.operator.IntSqlParameter
import com.losttemple.sql.language.operator.SqlParameter
import com.losttemple.sql.language.types.SetRef
import com.losttemple.sql.language.types.SqlInt
import java.sql.Types


class IntColumn(private val setReference: SetRef, name: String, private var precision: Int):
        SqlInt,
        CommonColumn<Int, IntColumn>(setReference, name) {
    private var isAutoIncrement: Boolean = false
    private var isSigned: Boolean = true

    override fun generateParameter(parameter: Int?): SqlParameter {
        return IntSqlParameter(parameter)
    }

    override val typeDescription: SqlTypeDescription
        get() {
            return SqlTypeDescription(
                    Types.BIT,
                    isNullable = isNullable,
                    isSearchable = isSearchable,
                    isSigned = isSigned,
                    precision = precision)
        }
}