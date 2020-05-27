package com.losttemple.sql.language.generate

interface AssignConstructor {
    fun addValue(column: String): ExpressionConstructor
}

class DefaultInsertConstructor(private val table: String):
        AssignConstructor, ExpressionSource, HashTarget {
    private val values: MutableMap<String, DefaultExpressionConstructor> = HashMap()
    private var source: DefaultSourceEvaluator = object: DefaultSourceEvaluator() {
        override fun evaluate(context: EvaluateContext): EC {
            val idGenerator = context.idGenerator
            val dialect = context.dialect
            val id = idGenerator.nextId()
            dialect.table(table)
            dialect.rename(id)
            return EC(mapOf(), id, mapOf(), mapOf())
        }

        override fun reference(): REC {
            return REC(mapOf())
        }
    }

    override fun addValue(column: String): ExpressionConstructor {
        val constructor = DefaultExpressionConstructor(this, null)
        values[column] = constructor
        return constructor
    }

    private fun hash1(): HashSqlSegment {
        val dialect = HashDialect()
        val idGenerator = CountIdGenerator()
        val context = EvaluateContext(dialect, idGenerator, mapOf(), mapOf())
        source.allEvaluator(context)
        return dialect.hash()
    }

    override fun hash(): Map<HashSqlSegment, HashTarget> {
        val result = HashMap<HashSqlSegment, HashTarget>()
        val currentHash = hash1()
        result[currentHash] = this
        return result
    }

    override fun findSource(path: String): SourceEvaluatorWithReference? {
        if (path == "") {
            return source
        }
        return null
    }

    fun root(outsideContext: EvaluateContext) {
        val dialect = outsideContext.dialect
        val idGenerator = outsideContext.idGenerator
        dialect.columnList()
        var first = true
        val columnValues = ArrayList<MutableMap.MutableEntry<String, DefaultExpressionConstructor>>(values.entries)
        for (columnValue in columnValues) {
            dialect.column(columnValue.key)
            if (first) {
                first = false
            }
            else {
                dialect.addToList()
            }
        }
        dialect.columnList()
        first = true
        for (columnValue in columnValues) {
            columnValue.value.evaluate(outsideContext)
            if (first) {
                first = false
            }
            else {
                dialect.addToList()
            }
        }
        dialect.insertWithColumns(table)
    }
}

class DefaultUpdateConstructor(private val table: String):
        AssignConstructor, ExpressionSource, HashTarget {
    private val values: MutableMap<String, DefaultExpressionConstructor> = HashMap()
    private var source: DefaultSourceEvaluator = object: DefaultSourceEvaluator() {
        override fun evaluate(context: EvaluateContext): EC {
            val idGenerator = context.idGenerator
            val dialect = context.dialect
            val id = idGenerator.nextId()
            dialect.table(table)
            dialect.rename(id)
            return EC(mapOf(), id, mapOf(), mapOf())
        }

        override fun reference(): REC {
            return REC(mapOf())
        }
    }

    override fun addValue(column: String): ExpressionConstructor {
        val constructor = DefaultExpressionConstructor(this, null)
        values[column] = constructor
        return constructor
    }

    private fun hash1(): HashSqlSegment {
        val dialect = HashDialect()
        val idGenerator = CountIdGenerator()
        val context = EvaluateContext(dialect, idGenerator, mapOf(), mapOf())
        source.allEvaluator(context)
        return dialect.hash()
    }

    override fun hash(): Map<HashSqlSegment, HashTarget> {
        val result = HashMap<HashSqlSegment, HashTarget>()
        val currentHash = hash1()
        result[currentHash] = this
        return result
    }

    override fun findSource(path: String): SourceEvaluatorWithReference? {
        if (path == "") {
            return source
        }
        return null
    }

    fun root(outsideContext: EvaluateContext) {
        val dialect = outsideContext.dialect
        val idGenerator = outsideContext.idGenerator
        val vs = source as SourceEvaluatorWithReference
        val pathMapping = mapOf(vs to table)
        val context = EvaluateContext(dialect, idGenerator, outsideContext.exports + pathMapping,
                outsideContext.referenceMapping)
        dialect.table(table)
        dialect.assignList()
        var first = true
        for (columnValue in values) {
            dialect.column(columnValue.key)
            columnValue.value.evaluate(context)
            dialect.assign()
            if (first) {
                first = false
            }
            else {
                dialect.addToList()
            }
        }
        dialect.updateAll()
    }
}