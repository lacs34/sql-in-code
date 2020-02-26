package com.losttemple.sql.language.operator

import com.losttemple.sql.language.generate.OrderStatus
import com.losttemple.sql.language.generate.SourceReference
import com.losttemple.sql.language.generate.withContract
import com.losttemple.sql.language.types.*

class OrderSet<T, K: Comparable<K>>(private val sourceSet: DbSet<T>, private val desc: Boolean): DbSet<T>, SqlSet {
    override val description: T = sourceSet.wrapDescription(DummyPrefix(this))
    lateinit var keys: List<SqlType<K>>

    override fun wrapDescription(prefix: SetPrefix): T {
        return sourceSet.wrapDescription(prefix)
    }

    override val set: SqlSet
        get() = this

    override fun push(constructor: SourceReference) {
        sourceSet.set.push(constructor)
        if (constructor.hasLimit) {
            embedContract(constructor, constructor.order) {
                for (key in keys) {
                    key.push(this)
                }
            }
        }
        else {
            constructor.order.withContract {
                for (key in keys) {
                    key.push(this)
                }
            }
        }
        constructor.orderStatus = if (desc) {
            OrderStatus.Descending
        }
        else {
            OrderStatus.Ascending
        }
    }
}

fun <T, K: Comparable<K>> DbSet<T>.order(predicate: T.()-> SqlType<K>): DbSet<T> {
    val orderSet = OrderSet<T, K>(this, false)
    val key = description.predicate()
    orderSet.keys = listOf(key)
    return orderSet
}

fun <T, K: Comparable<K>> DbSet<T>.order(firstPredicate: T.()-> SqlType<K>, vararg predicate: T.()-> SqlType<K>): DbSet<T> {
    val orderSet = OrderSet<T, K>(this, false)
    val keys = listOf(firstPredicate, *predicate).map { description.it() }
    orderSet.keys = keys
    return orderSet
}

fun <T, K: Comparable<K>> DbSet<T>.orderDesc(predicate: T.()-> SqlType<K>): DbSet<T> {
    val orderSet = OrderSet<T, K>(this, true)
    val key = description.predicate()
    orderSet.keys = listOf(key)
    return orderSet
}

fun <T, K: Comparable<K>> DbSet<T>.orderDesc(firstPredicate: T.()-> SqlType<K>, vararg predicate: T.()-> SqlType<K>): DbSet<T> {
    val orderSet = OrderSet<T, K>(this, true)
    val keys = listOf(firstPredicate, *predicate).map { description.it() }
    orderSet.keys = keys
    return orderSet
}