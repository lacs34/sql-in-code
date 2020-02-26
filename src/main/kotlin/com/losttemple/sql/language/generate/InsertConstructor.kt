package com.losttemple.sql.language.generate

interface InsertConstructor {
    var table: String
    fun addValue(column: String): ExpressionConstructor
}

interface UpdateConstructor {
    var table: String
    val where: ExpressionConstructor
    val whereAlwaysTrue: Boolean
    fun addAssign(column: String): ExpressionConstructor
}