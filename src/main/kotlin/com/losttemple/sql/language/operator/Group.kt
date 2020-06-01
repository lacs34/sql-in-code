package com.losttemple.sql.language.operator

import com.losttemple.sql.language.generate.*
import com.losttemple.sql.language.types.*
import java.sql.ResultSet

class GroupSet<T, K>(private val sourceSet: DbSet<T>): DbSet<T>, SqlSet {
    override val description: T = sourceSet.wrapDescription(DummyPrefix(this).append { it.group() })
    lateinit var keys: List<SqlType<K>>

    override fun wrapDescription(prefix: SetPrefix): T {
        val newRef = prefix.append { it.group() }
        return sourceSet.wrapDescription(newRef)
    }

    override val set: SqlSet
        get() = this

    override fun push(constructor: SourceReference) {
        sourceSet.set.push(constructor)
        if (constructor.groupStatus != GroupStatus.None || constructor.limitStatus != LimitStatus.None) {
            embedContract(constructor, constructor.group) {
                for (key in keys) {
                    key.push(this)
                }
            }
        }
        else if (constructor.hasOrder) {
            constructor.group.withContract {
                for (key in keys) {
                    key.push(this)
                }
            }
            constructor.order.clearOrder()
        }
        else {
            constructor.group.withContract {
                for (key in keys) {
                    key.push(this)
                }
            }
        }
    }
}

class GroupSetSelector<T, K, R>(
        private val groupSet: GroupSet<T, K>,
        private val selector: GroupOperatorEnvironment<T, K>.() -> R,
        private val keySelectors: List<T.()-> SqlType<K>>): DbSet<R>, SqlSet {
    override val description: R = selectResult(groupSet.description, DummyRef(this))
    override val set: SqlSet
        get() = this

    private fun selectResult(sourceDescription: T, setRef: SetRef): R {
        val keys = keySelectors.map { sourceDescription.it() }
        val environment = GroupOperatorEnvironment(keys, sourceDescription, setRef)
        return environment.selector()
    }

    override fun wrapDescription(prefix: SetPrefix): R {
        val rr = prefix.fillPath(DummyRef(this))
        return selectResult(groupSet.wrapDescription(prefix), rr)
    }

    override fun push(constructor: SourceReference) {
        groupSet.push(constructor)
    }
}

class SingleKeyGroupSetSelector<T, K, R>(
        private val groupSet: GroupSet<T, K>,
        private val selector: SingleKeyGroupOperatorEnvironment<T, K>.() -> R,
        private val keySelector: T.()-> SqlType<K>): DbSet<R>, SqlSet {
    override val description: R = selectResult(groupSet.description, DummyRef(this))
    override val set: SqlSet
        get() = this

    private fun selectResult(sourceDescription: T, setRef: SetRef): R {
        val key = sourceDescription.keySelector()
        val environment = SingleKeyGroupOperatorEnvironment(key, sourceDescription, setRef)
        return environment.selector()
    }

    override fun wrapDescription(prefix: SetPrefix): R {
        val rr = prefix.fillPath(DummyRef(this))
        return selectResult(groupSet.wrapDescription(prefix), rr)
    }

    override fun push(constructor: SourceReference) {
        groupSet.push(constructor)
    }
}

class GroupKey<T>(private val key: SqlType<T>, private val setRef: SetRef): SqlType<T> {
    override val reference: Collection<SetRef>
        get() = key.reference

    override fun push(constructor: ExpressionConstructor) {
        key.push(constructor)
        constructor.pushDown({ setRef.set.push(it) }) {
            setRef.reference(it)
        }
    }

    override fun getFromResult(dialect: SqlDialect, result: ResultSet, name: String): T? {
        return key.getFromResult(dialect, result, name)
    }

}

class GroupOperatorEnvironment<T, KEY_TYPE>(
        private val keys: List<SqlType<KEY_TYPE>>,
        private val source: T,
        private val ref: SetRef
) {
    fun keys(): List<SqlType<KEY_TYPE>> {
        return keys.map { GroupKey(it, ref) }
    }

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

class SingleKeyGroupOperatorEnvironment<T, KEY_TYPE>(
        private val key: SqlType<KEY_TYPE>,
        private val source: T,
        private val ref: SetRef
) {
    fun key(): SqlType<KEY_TYPE> {
        return GroupKey(key, ref)
    }

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

class GroupWithKey<T, KEY>(
        val sourceSet: DbSet<T>,
        val keys: List<T.()-> SqlType<KEY>>
) {
    fun <R> select(selector: GroupOperatorEnvironment<T, KEY>.() -> R): DbSet<R> {
        val groupSet = GroupSet<T, KEY>(sourceSet)
        val keyValues = keys.map { sourceSet.description.it() }
        groupSet.keys = keyValues
        return GroupSetSelector(groupSet, selector, keys)
    }
}

class GroupWithSingleKey<T, KEY>(
        val sourceSet: DbSet<T>,
        val key: T.()-> SqlType<KEY>
) {
    fun <R> select(selector: SingleKeyGroupOperatorEnvironment<T, KEY>.() -> R): DbSet<R> {
        val groupSet = GroupSet<T, KEY>(sourceSet)
        val keyValue = sourceSet.description.key()
        groupSet.keys = listOf(keyValue)
        return SingleKeyGroupSetSelector(groupSet, selector, key)
    }
}

fun <T, R> DbSet<T>.group(predicate: T.()-> SqlType<R>): GroupWithSingleKey<T, R> {
    return GroupWithSingleKey(this, predicate)
}

fun <T, R> DbSet<T>.group(firstPredicate: T.()-> SqlType<R>, vararg predicate: T.()-> SqlType<R>): GroupWithKey<T, R> {
    return GroupWithKey(this, listOf(firstPredicate, *predicate))
}