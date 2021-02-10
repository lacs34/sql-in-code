package com.losttemple.sql.language.generate

import java.util.*
import kotlin.collections.HashSet

data class EC(
        val pathMapping: Map<SourceEvaluatorWithReference, String>,
        val selfMapping: String?,
        val referenceMapping: Map<SourceEvaluatorWithReference, Map<String, String>>,
        val selfReference: Map<SourceEvaluatorWithReference, Set<String>>
)

data class ECR(
        val pathMapping: Map<SourceEvaluatorWithReference, String>,
        val referenceMapping: Map<SourceEvaluatorWithReference, Map<String, String>>,
        val selfReference: Map<SourceEvaluatorWithReference, Set<String>>
)

data class ECQ(
        val pathMapping: Map<SourceEvaluatorWithReference, String>,
        val referenceMapping: Map<SourceEvaluatorWithReference, Map<String, String>>
)

data class REC(
        val other: Map<String, SourceEvaluatorWithReference>
)

interface SourceEvaluatorWithReference {
    fun evaluate(context: EvaluateContext): EC
    fun reference(): REC
    val referencedColumns: MutableSet<String>
    fun pushOperator(operator: (EvaluateContext) -> Unit): String {
        error("not allowed to push operator to evaluator except embed operator")
    }
}

interface HashTarget {
    fun findSource(path: String): SourceEvaluatorWithReference?
}

abstract class DefaultSourceEvaluator: SourceEvaluatorWithReference {
    override val referencedColumns: MutableSet<String> = HashSet()
}

fun SourceEvaluatorWithReference.allReference(): REC {
    val subReference = reference()
    if (subReference.other.containsKey("")) {
        return subReference
    }
    return REC(subReference.other + mapOf("" to this))
}

fun SourceEvaluatorWithReference.allEvaluator(context: EvaluateContext): ECR {
    val subMapping = evaluate(context)
    val pathMapping = if (subMapping.selfMapping == null) {
        subMapping.pathMapping
    }
    else {
        subMapping.pathMapping + mapOf(this to subMapping.selfMapping)
    }
    val referenceMapping = if (referencedColumns.isEmpty()) {
        subMapping.selfReference
    }
    else {
        subMapping.selfReference + mapOf(this to referencedColumns)
    }
    return ECR(pathMapping, subMapping.referenceMapping, referenceMapping)
}

interface IdGenerator {
    fun nextId(): String
}

class DefaultSourceConstructor(source: Stack<SourceEvaluatorWithReference>, private val expressionSource: DefaultQueryConstructor): SourceConstructor {
    private val items: Stack<SourceEvaluatorWithReference> = source

    constructor(expressionSource: DefaultQueryConstructor): this(Stack(), expressionSource)

    fun popTop(): SourceEvaluatorWithReference {
        return items.pop()
    }

    fun pushInto(source: SourceEvaluatorWithReference) {
        items.push(source)
    }

    override fun table(name: String) {
        val sourceReference = object: DefaultSourceEvaluator() {
            override fun evaluate(context: EvaluateContext): EC {
                val idGenerator = context.idGenerator
                val dialect = context.dialect
                val id = idGenerator.nextId()
                dialect.table(name)
                dialect.rename(id)
                return EC(mapOf(), id, mapOf(), mapOf())
            }

            override fun reference(): REC {
                return REC(mapOf())
            }
        }
        items.push(sourceReference)
    }

    override fun leftJoin(): ExpressionConstructor {
        val right = items.pop()
        val left = items.pop()
        val condition = DefaultExpressionConstructor(expressionSource, null)
        val sourceReference = object: DefaultSourceEvaluator() {
            override fun evaluate(context: EvaluateContext): EC {
                val leftExports = left.allEvaluator(context)
                val rightExports = right.allEvaluator(context)
                val exports = leftExports.pathMapping + rightExports.pathMapping
                val context2 = EvaluateContext(
                        context.dialect,
                        context.idGenerator,
                        exports,
                        leftExports.referenceMapping + rightExports.referenceMapping)
                if (condition.isEmpty()) {
                    context2.dialect.now()
                }
                else {
                    condition.evaluate(context2)
                }
                context.dialect.leftJoin()
                return EC(
                        exports,
                        null,
                        leftExports.referenceMapping + rightExports.referenceMapping,
                        leftExports.selfReference + rightExports.selfReference)
            }

            override fun reference(): REC {
                val leftExports = left.allReference()
                val leftOther = leftExports.other.mapKeys { "L" + it.key }
                val rightExports = right.allReference()
                val rightSelf = rightExports.other.mapKeys { "R" + it.key }
                return REC(leftOther + rightSelf)
            }
        }
        items.push(sourceReference)
        return condition
    }

    override fun rightJoin(): ExpressionConstructor {
        val right = items.pop()
        val left = items.pop()
        val condition = DefaultExpressionConstructor(expressionSource, null)
        val sourceReference = object: DefaultSourceEvaluator() {
            override fun evaluate(context: EvaluateContext): EC {
                val leftExports = left.allEvaluator(context)
                val rightExports = right.allEvaluator(context)
                val exports = leftExports.pathMapping + rightExports.pathMapping
                val context2 = EvaluateContext(
                        context.dialect,
                        context.idGenerator,
                        exports,
                        leftExports.referenceMapping + rightExports.referenceMapping)
                if (condition.isEmpty()) {
                    context2.dialect.now()
                }
                else {
                    condition.evaluate(context2)
                }
                context.dialect.rightJoin()
                return EC(
                        exports,
                        null,
                        leftExports.referenceMapping + rightExports.referenceMapping,
                        leftExports.selfReference + rightExports.selfReference)
            }

            override fun reference(): REC {
                val leftExports = left.allReference()
                val leftOther = leftExports.other.mapKeys { "L" + it.key }
                val rightExports = right.allReference()
                val rightSelf = rightExports.other.mapKeys { "R" + it.key }
                return REC(leftOther + rightSelf)
            }
        }
        items.push(sourceReference)
        return condition
    }

    override fun innerJoin(): ExpressionConstructor {
        val right = items.pop()
        val left = items.pop()
        val condition = DefaultExpressionConstructor(expressionSource, null)
        val sourceReference = object: DefaultSourceEvaluator() {
            override fun evaluate(context: EvaluateContext): EC {
                val leftExports = left.allEvaluator(context)
                val rightExports = right.allEvaluator(context)
                val exports = leftExports.pathMapping + rightExports.pathMapping
                val context2 = EvaluateContext(
                        context.dialect,
                        context.idGenerator,
                        exports,
                        leftExports.referenceMapping + rightExports.referenceMapping)
                if (condition.isEmpty()) {
                    context2.dialect.now()
                }
                else {
                    condition.evaluate(context2)
                }
                context.dialect.innerJoin()
                return EC(
                        exports,
                        null,
                        leftExports.referenceMapping + rightExports.referenceMapping,
                        leftExports.selfReference + rightExports.selfReference)
            }

            override fun reference(): REC {
                val leftExports = left.allReference()
                val leftOther = leftExports.other.mapKeys { "L" + it.key }
                val rightExports = right.allReference()
                val rightSelf = rightExports.other.mapKeys { "R" + it.key }
                return REC(leftOther + rightSelf)
            }
        }
        items.push(sourceReference)
        return condition
    }

    override fun outerJoin(): ExpressionConstructor {
        val right = items.pop()
        val left = items.pop()
        val condition = DefaultExpressionConstructor(expressionSource, null)
        val sourceReference = object: DefaultSourceEvaluator() {
            override fun evaluate(context: EvaluateContext): EC {
                val leftExports = left.allEvaluator(context)
                val rightExports = right.allEvaluator(context)
                val exports = leftExports.pathMapping + rightExports.pathMapping
                val context2 = EvaluateContext(
                        context.dialect,
                        context.idGenerator,
                        exports,
                        leftExports.referenceMapping + rightExports.referenceMapping)
                condition.evaluate(context2)
                context.dialect.outerJoin()
                return EC(
                        exports,
                        null,
                        leftExports.referenceMapping + rightExports.referenceMapping,
                        leftExports.selfReference + rightExports.selfReference)
            }

            override fun reference(): REC {
                val leftExports = left.allReference()
                val leftOther = leftExports.other.mapKeys { "L" + it.key }
                val rightExports = right.allReference()
                val rightSelf = rightExports.other.mapKeys { "R" + it.key }
                return REC(leftOther + rightSelf)
            }
        }
        items.push(sourceReference)
        return condition
    }

    fun pushEmbedQuery(query: DefaultQueryConstructor) {
        val sourceReference = object: DefaultSourceEvaluator() {
            override fun evaluate(context: EvaluateContext): EC {
                val exports = query.output(context)
                val queryId = context.idGenerator.nextId()
                context.dialect.rename(queryId)
                return EC(exports.pathMapping.mapValues { queryId }, queryId, exports.referenceMapping, mapOf())
            }

            override fun reference(): REC {
                return query.queryReference()
            }

            override fun pushOperator(operator: (EvaluateContext) -> Unit): String {
                return query.addSelectOperator(operator, this)
            }
        }
        items.push(sourceReference)
    }

    override fun embedQuery(): QueryConstructor {
        val query = DefaultQueryConstructor(expressionSource, ParentKind.Source)
        val sourceReference = object: DefaultSourceEvaluator() {
            override fun evaluate(context: EvaluateContext): EC {
                val exports = query.output(context)
                val queryId = context.idGenerator.nextId()
                context.dialect.rename(queryId)
                return EC(exports.pathMapping.mapValues { queryId }, queryId, exports.referenceMapping, mapOf())
            }

            override fun reference(): REC {
                return query.queryReference()
            }

            override fun pushOperator(operator: (EvaluateContext) -> Unit): String {
                return query.addSelectOperator(operator, this)
            }
        }
        items.push(sourceReference)
        return query
    }

    override fun turnToEmbed(): QueryConstructor {
        val query = DefaultQueryConstructor(expressionSource, ParentKind.Source)
        query.replaceSource(items)

        val sourceReference = object: DefaultSourceEvaluator() {
            override fun evaluate(context: EvaluateContext): EC {
                val exports = query.output(context)
                val queryId = context.idGenerator.nextId()
                context.dialect.rename(queryId)
                return EC(exports.pathMapping.mapValues { queryId }, queryId, exports.referenceMapping, mapOf())
            }

            override fun reference(): REC {
                return query.queryReference()
            }

            override fun pushOperator(operator: (EvaluateContext) -> Unit): String {
                return query.addSelectOperator(operator, this)
            }
        }
        items.push(sourceReference)
        return query
    }

    fun evaluate(context: EvaluateContext): ECR {
        val item = items.peek()
        return item.allEvaluator(context)
    }

    fun reference(): REC {
        val item = items.peek()
        return item.allReference()
    }
}