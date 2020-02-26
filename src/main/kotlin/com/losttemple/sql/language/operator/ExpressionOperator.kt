package com.losttemple.sql.language.operator

import com.losttemple.sql.language.generate.ExpressionConstructor
import com.losttemple.sql.language.types.*
import java.time.Duration
import java.util.*

class NowValue: SqlDate {
    override val reference: Collection<SetRef>
        get() = listOf()

    override fun push(constructor: ExpressionConstructor) {
        constructor.now()
    }
}

fun now(): SqlType<Date> {
    return NowValue()
}

class DateWithOffset(private val left: SqlType<Date>, private val right: Duration, private val add: Boolean): SqlDate {
    override val reference: Collection<SetRef>
        get() = left.reference

    override fun push(constructor: ExpressionConstructor) {
        left.push(constructor)
        if (add) {
            constructor.addPeriod(right)
        }
        else {
            constructor.subPeriod(right)
        }
    }
}

operator fun SqlType<Date>.plus(another: Duration): SqlType<Date> {
    return DateWithOffset(this, another, true)
}

operator fun SqlType<Date>.minus(another: Duration): SqlType<Date> {
    return DateWithOffset(this, another, false)
}

class ColumnsLogicalAnd(private val left: SqlType<Boolean>, private val right: SqlType<Boolean>): SqlBool {
    override val reference: Collection<SetRef>
        get() = left.reference + right.reference

    override fun push(constructor: ExpressionConstructor) {
        left.push(constructor)
        right.push(constructor)
        constructor.and()
    }
}

class ColumnsLogicalOr(private val left: SqlType<Boolean>, private val right: SqlType<Boolean>): SqlBool {
    override val reference: Collection<SetRef>
        get() = left.reference + right.reference

    override fun push(constructor: ExpressionConstructor) {
        left.push(constructor)
        right.push(constructor)
        constructor.or()
    }
}

infix fun SqlType<Boolean>.and(another: SqlType<Boolean>): SqlType<Boolean> {
    return ColumnsLogicalAnd(this, another)
}

infix fun SqlType<Boolean>.or(another: SqlType<Boolean>): SqlType<Boolean> {
    return ColumnsLogicalOr(this, another)
}