package com.losttemple.sql.language.operator.sources

import com.losttemple.sql.language.operator.SqlParameter
import com.losttemple.sql.language.types.SqlType

data class SqlTypeDescription(
        val type: Int, // see java.sql.Types
        val isAutoIncrement: Boolean = false,
        val isCaseSensitive: Boolean = true,
        val isSearchable: Boolean = true,
        val isNullable: Boolean = false,
        val isSigned: Boolean = true,
        val precision: Int = 0,
        val scale: Int = 0
)

interface SourceColumn<T>: SqlType<T> {
    val name: String
    fun generateParameter(parameter: T?): SqlParameter
}

interface SqlSourceColumn<T>: SourceColumn<T> {
    val typeDescription: SqlTypeDescription
}