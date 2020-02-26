package com.losttemple.sql.language.generate

import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap

interface QueryConstructor: SourceReference {
    fun addSelector(): ExpressionConstructor
}

enum class ParentKind {
    Select,
    Source
}

class DefaultQueryConstructor(private val parent: DefaultQueryConstructor?, private val parentKind: ParentKind): QueryConstructor, ExpressionSource {
    constructor(): this(null, ParentKind.Select)

    private val select: MutableList<DefaultExpressionConstructor> = ArrayList()
    private var source: DefaultSourceConstructor = DefaultSourceConstructor(this)
    private var whereConstructor = DefaultExpressionConstructor(this, null)
    private var havingConstructor = DefaultExpressionConstructor(this, null)
    private var limitCount = 0
    private var isLimit = false
    private var groupKey = DefaultExpressionConstructor(this, null)
    private var isAggregate = false
    private var orderKey = DefaultExpressionConstructor(this, null)
    private var orderStatusValue = OrderStatus.None
    private val selectOperator = HashMap<String, (EvaluateContext) -> Unit>()
    private var selectOperatorCount = 0
    private var sourceEvaluator: SourceEvaluatorWithReference? = null

    fun addSelectOperator(operator: (EvaluateContext) -> Unit, source: SourceEvaluatorWithReference): String {
        if (groupStatus == GroupStatus.None) {
            error("only group query can accept select operator")
        }
        if (sourceEvaluator != null) {
            assert(source == sourceEvaluator)
        }
        else {
            sourceEvaluator = source
        }
        val id = "operator$selectOperatorCount"
        selectOperatorCount++
        selectOperator[id] = operator
        return id
    }

    fun referenceColumn(name: String) {
        val constructor = DefaultExpressionConstructor(this, null)
        select.add(constructor)
        constructor.addOperator {
            it.dialect.column(name)
        }
    }

    override fun addSelector(): ExpressionConstructor {
        val constructor = DefaultExpressionConstructor(this, null)
        select.add(constructor)
        return constructor
    }

    fun replaceSource(replacement: Stack<SourceEvaluatorWithReference> = Stack()) {
        source = DefaultSourceConstructor(replacement, this)
    }

    override val from: SourceConstructor
        get() = source

    override val where: ExpressionConstructor
        get() = whereConstructor

    override val whereAlwaysTrue: Boolean
        get() = whereConstructor.isEmpty()

    override val having: ExpressionConstructor
        get() = havingConstructor

    override val havingAlwaysTrue: Boolean
        get() = havingConstructor.isEmpty()

    override var limit: Int
        get() = limitCount
        set(value) {
            isLimit = true
            limitCount = value
        }

    override val hasLimit: Boolean
        get() = isLimit

    override val group: ExpressionConstructor
        get() = groupKey

    override fun aggregate() {
        isAggregate = true
    }

    override val groupStatus: GroupStatus
        get() {
            if (isAggregate) {
                return GroupStatus.Aggregate
            }
            else if (groupKey.isEmpty()) {
                return GroupStatus.None
            }
            return GroupStatus.Group
        }

    override fun turnToEmbed(): QueryConstructor {
        val embedQuery = DefaultQueryConstructor(this, ParentKind.Source)
        val topSource = source.popTop()
        embedQuery.source.pushInto(topSource)
        source.pushEmbedQuery(embedQuery)
        if (!whereAlwaysTrue) {
            val whereClone = whereConstructor.cloneAndClear(embedQuery)
            embedQuery.whereConstructor = whereClone
        }
        if (groupStatus == GroupStatus.Group) {
            val groupKeyClone = groupKey.cloneAndClear(embedQuery)
            embedQuery.groupKey = groupKeyClone
        }
        else if (groupStatus == GroupStatus.Aggregate) {
            embedQuery.isAggregate = true
        }
        if (!havingAlwaysTrue) {
            val havingClone = havingConstructor.cloneAndClear(embedQuery)
            embedQuery.havingConstructor = havingClone
        }
        if (orderStatus != OrderStatus.None) {
            val orderClone = orderKey.cloneAndClear(embedQuery)
            embedQuery.orderKey = orderClone
            embedQuery.orderStatus = orderStatus
            orderStatus = OrderStatus.None
        }
        if (isLimit) {
            embedQuery.limit = limitCount
            limit = 0
            isLimit = false
        }
        return embedQuery
    }

    override val order: ExpressionConstructor
        get() = orderKey

    override var orderStatus: OrderStatus
        get() = orderStatusValue
        set(value) {
            orderStatusValue = value
            if (value == OrderStatus.None) {
                orderKey.clear()
            }
        }

    fun hash1(): HashSqlSegment {
        val dialect = HashDialect()
        val idGenerator = CountIdGenerator()
        val context = EvaluateContext(dialect, idGenerator, mapOf(), mapOf())
        val exports = source.evaluate(context)
        val innerExports = EvaluateContext(dialect, idGenerator, exports.pathMapping, exports.referenceMapping)
        if (!whereAlwaysTrue) {
            whereConstructor.evaluate(innerExports)
            dialect.where()
        }
        if (groupStatus == GroupStatus.Group) {
            groupKey.evaluate(innerExports)
            dialect.group()
        }
        if (!havingAlwaysTrue) {
            havingConstructor.evaluate(innerExports)
            dialect.having()
        }
        if (orderStatus != OrderStatus.None) {
            orderKey.evaluate(innerExports)
            if (orderStatus == OrderStatus.Ascending) {
                dialect.order()
            }
            else {
                dialect.orderDesc()
            }
        }
        if (isLimit) {
            dialect.limit(limitCount)
        }
        return dialect.hash()
    }

    override fun hash(): Map<HashSqlSegment, DefaultQueryConstructor> {
        var currentQuery: DefaultQueryConstructor? = this
        var lastKind = ParentKind.Select
        val result = HashMap<HashSqlSegment, DefaultQueryConstructor>()
        while (currentQuery != null) {
            val query = currentQuery
            if (lastKind == ParentKind.Select) {
                val currentHash = query.hash1()
                result[currentHash] = currentQuery
            }
            lastKind = query.parentKind
            currentQuery = query.parent
        }
        return result
    }

    fun output(outsideContext: EvaluateContext): ECQ {
        val dialect = outsideContext.dialect
        val idGenerator = outsideContext.idGenerator
        val exports = source.evaluate(outsideContext)
        val context = EvaluateContext(
                dialect,
                idGenerator,
                outsideContext.exports + exports.pathMapping,
                outsideContext.referenceMapping + exports.referenceMapping)
        if (!whereAlwaysTrue) {
            whereConstructor.evaluate(context)
            dialect.where()
        }
        if (groupStatus == GroupStatus.Group) {
            groupKey.evaluate(context)
            dialect.group()
        }
        if (!havingAlwaysTrue) {
            havingConstructor.evaluate(context)
            dialect.having()
        }
        if (orderStatus != OrderStatus.None) {
            orderKey.evaluate(context)
            if (orderStatus == OrderStatus.Ascending) {
                dialect.order()
            }
            else {
                dialect.orderDesc()
            }
        }
        if (isLimit) {
            dialect.limit(limitCount)
        }
        dialect.columnList()
        var first = true
        val exportedReference = HashMap<SourceEvaluatorWithReference, Map<String, String>>()
        if (groupStatus == GroupStatus.None) {
            val exportToOutside = exports.selfReference
            for ((key, columns) in exportToOutside) {
                val selfColumnMapping = HashMap<String, String>()
                val name = exports.pathMapping[key]!!
                for (column in columns) {
                    dialect.column(name, column)
                    val newColumnName = context.idGenerator.nextId()
                    dialect.rename(newColumnName)
                    selfColumnMapping[column] = newColumnName
                    if (first) {
                        first = false
                    } else {
                        dialect.addToList()
                    }
                }
                exportedReference[key] = selfColumnMapping
            }
            for ((key, columns) in exports.referenceMapping) {
                val selfColumnMapping = HashMap<String, String>()
                val name = exports.pathMapping[key]!!
                for ((orgColumn, changedColumn) in columns) {
                    dialect.column(name, changedColumn)
                    val newColumnName = context.idGenerator.nextId()
                    dialect.rename(newColumnName)
                    selfColumnMapping[orgColumn] = newColumnName
                    if (first) {
                        first = false
                    } else {
                        dialect.addToList()
                    }
                }
                exportedReference[key] = selfColumnMapping
            }
            if (first) {
                dialect.now()
            }
            dialect.select()
            return ECQ(exports.pathMapping, exportedReference)
        }
        else {
            val evaluatorWithReference = sourceEvaluator
            if (evaluatorWithReference == null) {
                dialect.now()
                dialect.select()
                return ECQ(exports.pathMapping, mapOf())
            }
            val selfOperatorMapping = HashMap<String, String>()
            for ((key, value) in selectOperator) {
                value(context)
                val operatorId = context.idGenerator.nextId()
                context.dialect.rename(operatorId)
                selfOperatorMapping[key] = operatorId
                if (first) {
                    first = false
                } else {
                    dialect.addToList()
                }
            }
            exportedReference[evaluatorWithReference] = selfOperatorMapping
        }
        dialect.select()
        return ECQ(exports.pathMapping, exportedReference)
    }

    fun root(outsideContext: EvaluateContext): List<String> {
        val dialect = outsideContext.dialect
        val idGenerator = outsideContext.idGenerator
        val exports = source.evaluate(outsideContext)
        val context = EvaluateContext(
                dialect,
                idGenerator,
                outsideContext.exports + exports.pathMapping,
                outsideContext.referenceMapping + exports.referenceMapping)
        if (!whereAlwaysTrue) {
            whereConstructor.evaluate(context)
            dialect.where()
        }
        if (groupStatus == GroupStatus.Group) {
            groupKey.evaluate(context)
            dialect.group()
        }
        if (!havingAlwaysTrue) {
            havingConstructor.evaluate(context)
            dialect.having()
        }
        if (orderStatus != OrderStatus.None) {
            orderKey.evaluate(context)
            if (orderStatus == OrderStatus.Ascending) {
                dialect.order()
            }
            else {
                dialect.orderDesc()
            }
        }
        if (isLimit) {
            dialect.limit(limitCount)
        }
        dialect.columnList()
        var first = true
        val renameMapping = ArrayList<String>()
        for (selector in select) {
            selector.evaluate(context)
            if (first) {
                first = false
            }
            else {
                dialect.addToList()
            }
            val newName = idGenerator.nextId()
            dialect.rename(newName)
            renameMapping.add(newName)
        }
        dialect.select()
        return renameMapping
    }

    fun queryReference(): REC {
        val reference = source.reference()
        return if (groupStatus != GroupStatus.None) {
            val groupPath = reference.other.mapKeys { "G" + it.key }
            REC(groupPath)
        }
        else {
            reference
        }
    }
}