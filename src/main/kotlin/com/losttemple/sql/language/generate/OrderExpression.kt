package com.losttemple.sql.language.generate

import java.util.*
import kotlin.collections.ArrayList

interface OrderExpression {
    fun orderFirstAscending(): ExpressionConstructor
    fun orderFirstDescending(): ExpressionConstructor
    fun orderAscending(): ExpressionConstructor
    fun orderDescending(): ExpressionConstructor
    fun clearOrder()
    fun contract(): ContractOrderExpression
}

interface ContractOrderExpression: OrderExpression {
    fun commit()
}

class DefaultOrderExpression(
        private val source: ExpressionSource,
        private val parent: DefaultOrderExpression?,
        private var headKeys: MutableList<OrderKey>,
        private var keys: MutableList<OrderKey>): ContractOrderExpression {
    constructor(source: ExpressionSource, parent: DefaultOrderExpression?):
            this(source, parent, ArrayList(), ArrayList())

    private var clear = false

    fun cloneAndClear(expressionSource: ExpressionSource): DefaultOrderExpression {
        val clone = DefaultOrderExpression(expressionSource, null, headKeys, keys)
        headKeys = ArrayList()
        keys = ArrayList()
        return clone
    }

    fun push(context: EvaluateContext) {
        val dialect = context.dialect
        if (keys.isNotEmpty() || headKeys.isNotEmpty()) {
            var first = true
            for (key: OrderKey in headKeys.reversed()) {
                key.key.evaluate(context)
                if (key.orientation == OrderOrientation.Descending) {
                    dialect.descKey()
                }
                if (first) {
                    first = false
                }
                else {
                    dialect.addToList()
                }
            }
            for (key: OrderKey in keys) {
                key.key.evaluate(context)
                if (key.orientation == OrderOrientation.Descending) {
                    dialect.descKey()
                }
                if (first) {
                    first = false
                }
                else {
                    dialect.addToList()
                }
            }
            dialect.order()
        }
    }

    val hasOrder: Boolean
        get() {
            return if (parent == null) {
                keys.isNotEmpty() || headKeys.isNotEmpty()
            } else {
                (!clear && parent.hasOrder) || keys.isNotEmpty() || headKeys.isNotEmpty()
            }
        }

    override fun commit() {
        parent?: error("")
        if (clear) {
            parent.clearOrder()
        }
        parent.headKeys.addAll(headKeys)
        parent.keys.addAll(keys)
    }

    override fun orderFirstAscending(): ExpressionConstructor {
        val constructor = DefaultExpressionConstructor(source, null)
        headKeys.add(OrderKey(constructor, OrderOrientation.Ascending))
        return constructor
    }

    override fun orderFirstDescending(): ExpressionConstructor {
        val constructor = DefaultExpressionConstructor(source, null)
        headKeys.add(OrderKey(constructor, OrderOrientation.Descending))
        return constructor
    }

    override fun orderAscending(): ExpressionConstructor {
        val constructor = DefaultExpressionConstructor(source, null)
        keys.add(OrderKey(constructor, OrderOrientation.Ascending))
        return constructor
    }

    override fun orderDescending(): ExpressionConstructor {
        val constructor = DefaultExpressionConstructor(source, null)
        keys.add(OrderKey(constructor, OrderOrientation.Descending))
        return constructor
    }

    override fun clearOrder() {
        clear = true
        headKeys.clear()
        keys.clear()
    }

    override fun contract(): ContractOrderExpression {
        return DefaultOrderExpression(source, this)
    }
}