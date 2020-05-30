package com.losttemple.sql.language.operator

import com.losttemple.sql.language.generate.*
import com.losttemple.sql.language.types.*
import java.sql.ResultSet

class AggregateSet<T>(private val sourceSet: DbSet<T>): SqlSet {
    override fun push(constructor: SourceReference) {
        sourceSet.set.push(constructor)
        if (constructor.groupStatus != GroupStatus.None || constructor.limitStatus != LimitStatus.None) {
            constructor.turnToEmbed()
            constructor.aggregate()
        }
        else if (constructor.orderStatus != OrderStatus.None) {
            constructor.turnToEmbed()
            constructor.aggregate()
            constructor.orderStatus = OrderStatus.None
        }
        else {
            constructor.aggregate()
        }
    }
}

class MaxValue<T>(private val sourceValue: SqlType<T>, private val setRef: SetRef): SqlType<T> {
    override val reference: Collection<SetRef>
        get() = sourceValue.reference

    override fun push(constructor: ExpressionConstructor) {
        sourceValue.push(constructor)
        constructor.max()
        constructor.pushDown({ setRef.set.push(it) }) {
            setRef.reference(it)
        }
    }

    override fun getFromResult(dialect: SqlDialect, result: ResultSet, name: String): T? {
        return sourceValue.getFromResult(dialect, result, name)
    }
}

class MinValue<T>(private val sourceValue: SqlType<T>, private val setRef: SetRef): SqlType<T> {
    override val reference: Collection<SetRef>
        get() = sourceValue.reference

    override fun push(constructor: ExpressionConstructor) {
        sourceValue.push(constructor)
        constructor.min()
        constructor.pushDown({ setRef.set.push(it) }) {
            setRef.reference(it)
        }
    }

    override fun getFromResult(dialect: SqlDialect, result: ResultSet, name: String): T? {
        return sourceValue.getFromResult(dialect, result, name)
    }
}

class SumValue<T: Number>(private val sourceValue: SqlType<T>, private val setRef: SetRef): SqlType<T> {
    override val reference: Collection<SetRef>
        get() = sourceValue.reference

    override fun push(constructor: ExpressionConstructor) {
        sourceValue.push(constructor)
        constructor.sum()
        constructor.pushDown({ setRef.set.push(it) }) {
            setRef.reference(it)
        }
    }

    override fun getFromResult(dialect: SqlDialect, result: ResultSet, name: String): T? {
        return sourceValue.getFromResult(dialect, result, name)
    }
}

class CountValue<T>(private val sourceValue: SqlType<T>, private val setRef: SetRef): SqlType<Int>, SqlInt {
    override val reference: Collection<SetRef>
        get() = sourceValue.reference

    override fun push(constructor: ExpressionConstructor) {
        sourceValue.push(constructor)
        constructor.count()
        constructor.pushDown({ setRef.set.push(it) }) {
            setRef.reference(it)
        }
    }
}

class AggregateOperatorEnvironment<T>(private val source: T,
                                      private val ref: SetRef) {
    fun <R> max(value: T.() -> SqlType<R>): SqlType<R> where R: Comparable<R>{
        val parameter = source.value()
        return MaxValue(parameter, ref)
    }

    fun <R> min(value: T.() -> SqlType<R>): SqlType<R> where R: Comparable<R>{
        val parameter = source.value()
        return MinValue(parameter, ref)
    }

    fun <R: Number> sum(value: T.() -> SqlType<R>): SqlType<R>{
        val parameter = source.value()
        return SumValue(parameter, ref)
    }

    fun <R> count(value: T.() -> SqlType<R>): SqlType<Int> where R: Comparable<R>{
        val parameter = source.value()
        return CountValue(parameter, ref)
    }
}

class AggregateInstance<T, S>(override val description: T,
                           private val aggregateSet: AggregateSet<S>,
                           private val source: DbSet<S>,
                           private val handler: AggregateOperatorEnvironment<S>.()-> T): DbInstance<T> {
    override val set: SqlSet
        get() = aggregateSet

    override fun wrapDescription(prefix: SetPrefix): T {
        val environment = AggregateOperatorEnvironment(source.wrapDescription(prefix.append { it.group() }), prefix.fillPath(DummyRef(set)))
        return environment.handler()
    }
}

fun <T, R> DbSet<T>.aggregate(handler: AggregateOperatorEnvironment<T>.()->R): DbInstance<R> {
    val aggregateSet = AggregateSet(this)
    val prefix = DummyPrefix(aggregateSet)
    val aggregateDescription = wrapDescription(prefix.append { it.group() })
    val environment = AggregateOperatorEnvironment(aggregateDescription, DummyRef(aggregateSet))
    val instanceDescription = environment.handler()
    return AggregateInstance(instanceDescription, aggregateSet, this, handler)
}