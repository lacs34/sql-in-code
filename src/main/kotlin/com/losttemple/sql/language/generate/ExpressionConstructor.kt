package com.losttemple.sql.language.generate

import com.losttemple.sql.language.operator.SqlDialect
import java.time.Duration
import java.util.*
import kotlin.collections.ArrayList

class PathReferenceConstructor: ReferenceConstructor {
    var path = ""
        private set

    var column = ""
        private set

    override fun joinLeft(): ReferenceConstructor {
        path += "L"
        return this
    }

    override fun joinRight(): ReferenceConstructor {
        path += "R"
        return this
    }

    override fun group(): ReferenceConstructor {
        path += "G"
        return this
    }

    override fun column(name: String) {
        column = name
    }

}

data class EvaluateContext(
    val dialect: SqlDialect,
    val idGenerator: IdGenerator,
    val exports: Map<SourceEvaluatorWithReference, String>,
    val referenceMapping: Map<SourceEvaluatorWithReference, Map<String, String>>
)

interface ExpressionSource {
    fun hash(): Map<HashSqlSegment, HashTarget>
}

class DefaultExpressionConstructor(
        private val source: ExpressionSource,
        private val parent: DefaultExpressionConstructor?,
        private var operators: Stack<(EvaluateContext) -> Unit>): ContractExpression {
    private val delayedReference = ArrayList<Pair<SourceEvaluatorWithReference, String>>()
    private val delayedPush = ArrayList<Triple<SourceEvaluatorWithReference, (EvaluateContext) -> Unit, (String) -> Unit>>()

    constructor(source: ExpressionSource, parent: DefaultExpressionConstructor?): this(source, parent, Stack())
    fun cloneAndClear(expressionSource: ExpressionSource): DefaultExpressionConstructor {
        val clone = DefaultExpressionConstructor(expressionSource, null, operators)
        operators = Stack()
        return clone
    }

    fun clear() {
        operators.clear()
    }

    fun addOperator(addedOperator: (EvaluateContext) -> Unit) {
        operators.push(addedOperator)
    }

    fun isEmpty(): Boolean {
        return operators.isEmpty()
    }

    fun evaluate(context: EvaluateContext) {
        if (operators.size > 1) {
            var first = true
            for (dialectOperator in operators) {
                dialectOperator(context)
                if (first) {
                    first = false
                }
                else {
                    context.dialect.addToList()
                }
            }
        }
        else {
            for (dialectOperator in operators) {
                dialectOperator(context)
            }
        }
    }

    override fun constance(value: Int?) {
        operators.push{ it.dialect.constance(value) }
    }

    override fun constance(value: String?) {
        operators.push{ it.dialect.constance(value) }
    }

    override fun constance(value: Date?) {
        operators.push{ it.dialect.constance(value) }
    }

    override fun constance(value: Boolean?) {
        operators.push{ it.dialect.constance(value) }
    }

    override fun constance(value: Double?) {
        operators.push{ it.dialect.constance(value) }
    }

    override fun and() {
        val right = operators.pop()
        val left = operators.pop()
        operators.push{
            left(it)
            right(it)
            it.dialect.and()
        }
    }

    override fun or() {
        val right = operators.pop()
        val left = operators.pop()
        operators.push{
            left(it)
            right(it)
            it.dialect.or()
        }
    }

    override fun eq() {
        val right = operators.pop()
        val left = operators.pop()
        operators.push{
            left(it)
            right(it)
            it.dialect.eq() }
    }

    override fun greater() {
        val right = operators.pop()
        val left = operators.pop()
        operators.push{
            left(it)
            right(it)
            it.dialect.greater() }
    }

    override fun add() {
        val right = operators.pop()
        val left = operators.pop()
        operators.push{
            left(it)
            right(it)
            it.dialect.add() }
    }

    override fun subtraction() {
        val right = operators.pop()
        val left = operators.pop()
        operators.push{
            left(it)
            right(it)
            it.dialect.subtraction() }
    }

    override fun addPeriod(period: Duration) {
        val source = operators.pop()
        operators.push{
            source(it)
            it.dialect.addPeriod(period) }
    }

    override fun subPeriod(period: Duration) {
        val source = operators.pop()
        operators.push{
            source(it)
            it.dialect.subPeriod(period) }
    }

    override fun max() {
        val source = operators.pop()
        operators.push{
            source(it)
            it.dialect.max() }
    }

    override fun min() {
        val source = operators.pop()
        operators.push{
            source(it)
            it.dialect.min() }
    }

    override fun sum() {
        val source = operators.pop()
        operators.push{
            source(it)
            it.dialect.sum() }
    }

    override fun count() {
        val source = operators.pop()
        operators.push{
            source(it)
            it.dialect.count() }
    }

    override fun now() {
        operators.push{ it.dialect.now() }
    }

    override fun reference(sourceHash: (SourceReference) -> Unit, constructor: (ReferenceConstructor) -> Unit): Boolean {
        val query = DefaultQueryConstructor()
        sourceHash(query)
        val hash = query.hash1()
        val pathReference = PathReferenceConstructor()
        constructor(pathReference)
        val path = pathReference.path
        val exports = source.hash()
        val relatedSource = exports[hash] ?: return false
        val v = relatedSource.findSource(path) ?: return false
        if (parent == null) {
            v.referencedColumns.add(pathReference.column)
        }
        else {
            delayedReference.add(Pair(v, pathReference.column))
        }
        operators.push{
            try {
                val sourceName = it.exports[v]!!
                val column = (it.referenceMapping[v]?: mapOf())[pathReference.column] ?: pathReference.column
                it.dialect.column(sourceName, column)
            } catch (e: Throwable) {
                error("sss")
            }
        }
        return true
    }

    override fun pushDown(direction: (SourceReference) -> Unit, destination: (ReferenceConstructor) -> Unit) {
        val query = DefaultQueryConstructor()
        direction(query)
        val hash = query.hash1()
        val pathReference = PathReferenceConstructor()
        destination(pathReference)
        val path = pathReference.path
        val exports = source.hash()
        val relatedSource = exports[hash] ?: return
        val v = relatedSource.findSource(path) ?: return
        val operator = operators.last()
        operators.removeAt(operators.size - 1)
        var name = ""
        if (parent == null) {
            name = v.pushOperator(operator)
        }
        else {
            delayedPush.add(Triple(v, operator) {n -> name = n })
        }
        operators.push{
            val source = it.exports[v]!!
            val reference = it.referenceMapping[v]!![name]!!
            it.dialect.column(source, reference)
        }
    }

    override fun embedQuery(): QueryConstructor {
        val query = DefaultQueryConstructor()
        operators.push{ query.output(it) }
        return query
    }

    override fun contract(): ContractExpression {
        return DefaultExpressionConstructor(source, this)
    }

    override fun commit() {
        val existedParent = parent?: error("")
        existedParent.operators.addAll(operators)
        for (delay in delayedReference) {
            delay.first.referencedColumns.add(delay.second)
        }
        for (delay in delayedPush) {
            val res = delay.first.pushOperator(delay.second)
            delay.third(res)
        }
    }
}