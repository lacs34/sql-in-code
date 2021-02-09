package com.losttemple.sql.language.operator

import com.losttemple.sql.language.generate.*
import com.losttemple.sql.language.types.*

class IntermediateSet<T, S>(override val description: T,
                            private val sourceSet: DbSet<S>,
                            private val mapper: S.()->T): DbSet<T> {
    override fun wrapDescription(prefix: SetPrefix): T {
        val transferredSource = sourceSet.wrapDescription(prefix)
        return transferredSource.mapper()
    }

    override val set: SqlSet
        get() = sourceSet.set
}

fun <T, R> DbSet<T>.select(handler: T.()->R): DbSet<R> {
    val description = description.handler()
    return IntermediateSet(description, this, handler)
}

fun embedContract(source: SourceReference, expression: ExpressionConstructor, handler: ContractExpression.() -> Unit) {
    source.turnToEmbed()
    val contract = expression.contract()
    val embedConstructor = object: ContractExpression by contract {
        override fun reference(sourceHash: (SourceReference) -> Unit, constructor: (ReferenceConstructor) -> Unit): Boolean {
            return contract.reference({
                sourceHash(it)
                it.turnToEmbed()
            }, constructor)
        }

        override fun pushDown(direction: (SourceReference) -> Unit, destination: (ReferenceConstructor) -> Unit) {
            return contract.pushDown({
                direction(it)
                it.turnToEmbed()
            }, destination)
        }
    }
    embedConstructor.handler()
    contract.commit()
}

fun embedOrderContract(source: SourceReference, expression: OrderExpression, handler: ContractOrderExpression.() -> Unit) {
    source.turnToEmbed()
    val contract = expression.contract()
    val embedConstructor = object: ContractOrderExpression by contract {
        override fun orderAscending(): ExpressionConstructor {
            val ca = contract.orderAscending()
            return object: ExpressionConstructor by ca {
                override fun reference(sourceHash: (SourceReference) -> Unit, constructor: (ReferenceConstructor) -> Unit): Boolean {
                    return ca.reference({
                        sourceHash(it)
                        it.turnToEmbed()
                    }, constructor)
                }

                override fun pushDown(direction: (SourceReference) -> Unit, destination: (ReferenceConstructor) -> Unit) {
                    return ca.pushDown({
                        direction(it)
                        it.turnToEmbed()
                    }, destination)
                }
            }
        }

        override fun orderDescending(): ExpressionConstructor {
            val ca = contract.orderDescending()
            return object: ExpressionConstructor by ca {
                override fun reference(sourceHash: (SourceReference) -> Unit, constructor: (ReferenceConstructor) -> Unit): Boolean {
                    return ca.reference({
                        sourceHash(it)
                        it.turnToEmbed()
                    }, constructor)
                }

                override fun pushDown(direction: (SourceReference) -> Unit, destination: (ReferenceConstructor) -> Unit) {
                    return ca.pushDown({
                        direction(it)
                        it.turnToEmbed()
                    }, destination)
                }
            }
        }
    }
    embedConstructor.handler()
    contract.commit()
}

class FilteredSet<T>(private val sourceSet: DbSet<T>): DbSet<T>, SqlSet {
    override val description: T = sourceSet.wrapDescription(DummyPrefix(this))
    lateinit var condition: SqlType<Boolean>

    override fun wrapDescription(prefix: SetPrefix): T {
        return sourceSet.wrapDescription(prefix)
    }

    override val set: SqlSet
        get() = this


    override fun push(constructor: SourceReference) {
        sourceSet.set.push(constructor)
        if (constructor.limitStatus != LimitStatus.None || constructor.hasOrder) {
            embedContract(constructor, constructor.where) {
                condition.push(this)
            }
        }
        else {
            if (constructor.groupStatus == GroupStatus.None) {
                if (constructor.whereAlwaysTrue) {
                    constructor.where.withContract {
                        condition.push(this)
                    }
                } else {
                    constructor.where.withContract {
                        condition.push(this)
                    }
                    constructor.where.and()
                }
            } else if (constructor.groupStatus == GroupStatus.Group) {
                if (constructor.havingAlwaysTrue) {
                    constructor.having.withContract {
                        condition.push(this)
                    }
                } else {
                    constructor.having.withContract {
                        condition.push(this)
                    }
                    constructor.having.and()
                }
            }
        }
    }
}

infix fun <T> DbSet<T>.where(predicate: T.()->SqlType<Boolean>): DbSet<T> {
    val filteredSet = FilteredSet(this)
    val condition = description.predicate()
    filteredSet.condition = condition
    return filteredSet
}

class ColumnsEqualBool<T>(private val left: SqlType<T>, private val right: SqlType<T>): SqlBool {
    override val reference: Collection<SetRef>
        get() = left.reference + right.reference

    override fun push(constructor: ExpressionConstructor) {
        left.push(constructor)
        right.push(constructor)
        constructor.eq()
    }
}

class IntColumnConstEqualBool(private val left: SqlType<Int>, private val right: Int): SqlBool {
    override val reference: Collection<SetRef>
        get() = left.reference

    override fun push(constructor: ExpressionConstructor) {
        left.push(constructor)
        constructor.constance(right.toBigInteger())
        constructor.eq()
    }
}

class LongColumnConstEqualBool(private val left: SqlType<Long>, private val right: Long): SqlBool {
    override val reference: Collection<SetRef>
        get() = left.reference

    override fun push(constructor: ExpressionConstructor) {
        left.push(constructor)
        constructor.constance(right.toBigInteger())
        constructor.eq()
    }
}

class StringColumnConstEqualBool(private val left: SqlType<String>, private val right: String): SqlBool {
    override val reference: Collection<SetRef>
        get() = left.reference

    override fun push(constructor: ExpressionConstructor) {
        left.push(constructor)
        constructor.constance(right)
        constructor.eq()
    }
}

class BoolColumnConstEqualBool(private val left: SqlType<Boolean>, private val right: Boolean): SqlBool {
    override val reference: Collection<SetRef>
        get() = left.reference

    override fun push(constructor: ExpressionConstructor) {
        left.push(constructor)
        constructor.constance(right)
        constructor.eq()
    }
}

class ColumnsGreaterBool<T: Comparable<T>>(private val left: SqlType<T>, private val right: SqlType<T>): SqlBool {
    override val reference: Collection<SetRef>
        get() = left.reference + right.reference

    override fun push(constructor: ExpressionConstructor) {
        left.push(constructor)
        right.push(constructor)
        constructor.greater()
    }
}

class IntColumnConstGreaterBool(private val left: SqlType<Int>, private val right: Int): SqlBool {
    override val reference: Collection<SetRef>
        get() = left.reference

    override fun push(constructor: ExpressionConstructor) {
        left.push(constructor)
        constructor.constance(right.toBigInteger())
        constructor.greater()
    }
}

class DoubleColumnConstGreaterBool(private val left: SqlType<Double>, private val right: Double): SqlBool {
    override val reference: Collection<SetRef>
        get() = left.reference

    override fun push(constructor: ExpressionConstructor) {
        left.push(constructor)
        constructor.constance(right)
        constructor.greater()
    }
}

infix fun <T> SqlType<T>.eq(another: SqlType<T>): SqlBool {
    return ColumnsEqualBool(this, another)
}

infix fun SqlType<Int>.eq(another: Int): SqlBool {
    return IntColumnConstEqualBool(this, another)
}

infix fun SqlType<Long>.eq(another: Long): SqlBool {
    return LongColumnConstEqualBool(this, another)
}

infix fun SqlType<String>.eq(another: String): SqlBool {
    return StringColumnConstEqualBool(this, another)
}

infix fun SqlType<Boolean>.eq(another: Boolean): SqlBool {
    return BoolColumnConstEqualBool(this, another)
}

infix fun <T: Comparable<T>> SqlType<T>.gt(another: SqlType<T>): SqlBool {
    return ColumnsGreaterBool(this, another)
}

infix fun SqlType<Int>.gt(another: Int): SqlBool {
    return IntColumnConstGreaterBool(this, another)
}

infix fun SqlType<Double>.gt(another: Double): SqlBool {
    return DoubleColumnConstGreaterBool(this, another)
}

infix fun SqlType<Double>.gt(another: Number): SqlBool {
    return DoubleColumnConstGreaterBool(this, another.toDouble())
}
class CalcConfig(private val set: SqlSet) {
    private val columns: MutableList<SqlTypeCommon> = ArrayList()
    fun <T: SqlTypeCommon> need(column: T) {
        columns.add(column)
    }

    fun <T> generate(description: T): DbResult<T> {
        return DbResult(columns, description, set)
    }

    fun <T> generateInstance(description: T): DbInstanceResult<T> {
        return DbInstanceResult(columns, description, set)
    }
}