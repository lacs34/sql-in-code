package com.losttemple.sql.language.stream

import com.losttemple.sql.language.generate.ExpressionConstructor
import com.losttemple.sql.language.generate.OrderKey
import com.losttemple.sql.language.operator.DbSource
import com.losttemple.sql.language.operator.OrderSetKey
import com.losttemple.sql.language.types.SqlType
import com.losttemple.sql.language.types.SqlTypeCommon

interface SqlOperatorVisitor<T> {
    fun visitFilter(operator: SqlFilterOperator<T>)
    fun <S> visitMap(operator: SqlMapOperator<T, S>)
    fun <S: DbSource> visitSource(operator: SqlSourceOperator<S>)
    fun visitSort(operator: SqlSortOperator<T>)
    fun <L, R> visitJoin(operator: SqlJoinOperator<T, L, R>)
}

interface SqlOperator<T> {
    val result: T
    fun visit(visitor: SqlOperatorVisitor<T>)
}

interface SqlTransformOperator<T, S>: SqlOperator<T> {
    val source: SqlOperator<S>
}

interface SqlFilterOperator<T>: SqlTransformOperator<T, T> {
    var condition: SqlType<Boolean>

    override fun visit(visitor: SqlOperatorVisitor<T>) {
        visitor.visitFilter(this)
    }
}

interface SqlMapOperator<T, S>: SqlTransformOperator<T, S> {
    override fun visit(visitor: SqlOperatorVisitor<T>) {
        visitor.visitMap(this)
    }
}

interface SqlSourceOperator<T: DbSource>: SqlOperator<T> {
    val source: T

    override fun visit(visitor: SqlOperatorVisitor<T>) {
        visitor.visitSource(this)
    }
}

enum class SortOrder {
    Ascending,
    Descending
}

data class SortKey(
        val value: SqlTypeCommon,
        val order: SortOrder
)

interface SqlSortOperator<T>: SqlTransformOperator<T, T> {
    val keys: MutableList<SortKey>

    override fun visit(visitor: SqlOperatorVisitor<T>) {
        visitor.visitSort(this)
    }
}

interface SqlJoinOperator<T, L, R>: SqlOperator<T> {
    val left: SqlOperator<L>
    val right: SqlOperator<R>
    var condition: SqlType<Boolean>

    override fun visit(visitor: SqlOperatorVisitor<T>) {
        visitor.visitJoin(this)
    }
}