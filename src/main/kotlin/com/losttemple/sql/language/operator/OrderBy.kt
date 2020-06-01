package com.losttemple.sql.language.operator

import com.losttemple.sql.language.generate.*
import com.losttemple.sql.language.types.*

data class OrderSetKey<T>(
        val key: SqlType<T>,
        val orientation: OrderOrientation
)

class OrderSet<T, K: Comparable<K>>(private val sourceSet: DbSet<T>, private val desc: Boolean): DbSet<T>, SqlSet {
    override val description: T = sourceSet.wrapDescription(DummyPrefix(this))
    lateinit var keys: List<OrderSetKey<K>>

    override fun wrapDescription(prefix: SetPrefix): T {
        return sourceSet.wrapDescription(prefix)
    }

    override val set: SqlSet
        get() = this

    override fun push(constructor: SourceReference) {
        sourceSet.set.push(constructor)
        if (constructor.limitStatus != LimitStatus.None) {
            embedOrderContract(constructor, constructor.order) {
                for (key in keys) {
                    val order = if (key.orientation == OrderOrientation.Ascending) {
                        orderAscending()
                    }
                    else {
                        orderDescending()
                    }
                    key.key.push(order)
                }
            }
        }
        else if (constructor.hasOrder) {
            constructor.order.withContract {
                for (key in keys.reversed()) {
                    val order = if (key.orientation == OrderOrientation.Ascending) {
                        orderFirstAscending()
                    }
                    else {
                        orderFirstDescending()
                    }
                    key.key.push(order)
                }
            }
        }
        else {
            constructor.order.withContract {
                for (key in keys) {
                    val order = if (key.orientation == OrderOrientation.Ascending) {
                        orderAscending()
                    }
                    else {
                        orderDescending()
                    }
                    key.key.push(order)
                }
            }
        }
    }
}

fun <T, K: Comparable<K>> DbSet<T>.order(predicate: T.()-> SqlType<K>): DbSet<T> {
    val orderSet = OrderSet<T, K>(this, false)
    val key = description.predicate()
    orderSet.keys = listOf(OrderSetKey(key, OrderOrientation.Ascending))
    return orderSet
}

fun <T, K: Comparable<K>> DbSet<T>.order(firstPredicate: T.()-> SqlType<K>, vararg predicate: T.()-> SqlType<K>): DbSet<T> {
    val orderSet = OrderSet<T, K>(this, false)
    val keys = listOf(firstPredicate, *predicate).map { OrderSetKey(description.it(), OrderOrientation.Ascending) }
    orderSet.keys = keys
    return orderSet
}

fun <T, K: Comparable<K>> DbSet<T>.orderDesc(predicate: T.()-> SqlType<K>): DbSet<T> {
    val orderSet = OrderSet<T, K>(this, true)
    val key = description.predicate()
    orderSet.keys = listOf(OrderSetKey(key, OrderOrientation.Descending))
    return orderSet
}

fun <T, K: Comparable<K>> DbSet<T>.orderDesc(firstPredicate: T.()-> SqlType<K>, vararg predicate: T.()-> SqlType<K>): DbSet<T> {
    val orderSet = OrderSet<T, K>(this, true)
    val keys = listOf(firstPredicate, *predicate).map { OrderSetKey(description.it(), OrderOrientation.Descending) }
    orderSet.keys = keys
    return orderSet
}