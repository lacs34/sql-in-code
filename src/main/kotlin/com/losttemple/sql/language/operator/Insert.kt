package com.losttemple.sql.language.operator

/*
class DbInsertionEnvironment(private val table: String) {
    private data class ColumnValue(
        val column: String,
        val value: SqlParameter
    )
    private val columnValues = ArrayList<ColumnValue>()

    operator fun <T> SourceColumn<T>.invoke(value: T?) {
        columnValues.add(ColumnValue(name, generateParameter(value)))
    }

    fun execute(machine: SqlDialect, connection: Connection) {
        val generator = SqlGenerator(machine)
        var firstColumn = true
        machine.columnList()
        for (column in columnValues) {
            machine.column(column.column)
            if (!firstColumn) {
                machine.addToList()
            }
            firstColumn = false
        }
        var firstValue = true
        machine.columnList()
        val columnValueContext = ValueGenerateContext(mapOf(), SourcePath(""))
        for (column in columnValues) {
            column.value.setParam(generator, columnValueContext)
            if (!firstValue) {
                machine.addToList()
            }
            firstValue = false
        }
        machine.insertWithColumns(table)

        println(machine.sql.describe())
        val statement = machine.sql.prepare(connection)
        val affected = statement.executeUpdate()
        if (affected != 1) {
            error("")
        }
    }
}

class DbTableDescription<T: DbSource>(private val creator: ((TableConfigure.()->Unit)-> SetRef)->T) {
    fun insert(machine: SqlDialect, connection: Connection, handler: DbInsertionEnvironment.(T)->Unit) {
        val sourceConfig = SourceTableConfigure()
        val source = creator {
            sourceConfig.it()
            sourceConfig.toSource().reference
        }
        val set = source.reference.set as SourceSet
        val environment = DbInsertionEnvironment(set.name)
        environment.handler(source)
        environment.execute(machine, connection)
    }

    fun update(machine: SqlDialect, connection: Connection, handler: DbUpdateEnvironment.(T)->Unit) {
        val sourceConfig = SourceTableConfigure()
        val source = creator {
            sourceConfig.it()
            sourceConfig.toSource().reference
        }
        val set = source.reference.set as SourceSet
        val environment = DbUpdateEnvironment(set.name, null)
        environment.handler(source)
        environment.execute(machine, connection)
    }

    fun delete(machine: SqlDialect, connection: Connection) {
        val sourceConfig = SourceTableConfigure()
        val source = creator {
            sourceConfig.it()
            sourceConfig.toSource().reference
        }
        val set = source.reference.set as SourceSet
        machine.deleteAll(set.name)
        val statement = machine.sql.prepare(connection)
        statement.executeUpdate()
    }

    private class DbTableDescriptionSet<T: DbSource>(private val source: T, private val db: SourceSet,
                                                     override val set: SqlSet,
                                                     private val creator: ((TableConfigure.()->Unit)->SetRef)->T): DbSet<T> {
        override val description: T
            get() = source

        override fun wrapDescription(prefix: SetPrefix): T {
            return creator { prefix.fillPath(db.reference.path.path) }
        }
    }

    private class DbDummySet(private val set: SourceSet): SqlSet {
        override fun push(dialect: SqlGenerator, context: SetGenerateContext): Map<SourcePath, String> {
            return mapOf(SourcePath("table(${set.name})") to set.name)
        }
    }

    fun where(predicate: T.()-> SqlBool): FilteredDbTable<T> {
        val sourceConfig = SourceTableConfigure()
        val source = creator {
            sourceConfig.it()
            sourceConfig.toSource().reference
        }
        val set = source.reference.set as SourceSet
        val dummyDb = DbDummySet(set)
        val dummySource = creator {
            SetRef(dummyDb, SourcePath("table(${set.name})"))
        }

        val sourceSet = DbTableDescriptionSet<T>(dummySource, set, dummyDb, creator)
        val filteredSet = FilteredSet(sourceSet)
        val prefix = PrefixWithPath(filteredSet, "where")
        val resultDescription = sourceSet.wrapDescription(prefix)
        val condition = resultDescription.predicate()
        filteredSet.condition = condition
        return FilteredDbTable(resultDescription, set, condition)
    }
}

class FilteredDbTable<T: DbSource>(private val source: T, private val db: SourceSet, private val condition: SqlType<Boolean>) {
    fun update(machine: SqlDialect, connection: Connection, handler: DbUpdateEnvironment.(T)->Unit) {
        val environment = DbUpdateEnvironment(db.name, condition)
        environment.handler(source)
        environment.execute(machine, connection)
    }

    fun delete(machine: SqlDialect, connection: Connection) {
        val table = db.name
        val generator = SqlGenerator(machine)
        machine.table(table)
        val generator2 = generator.withSource(mapOf(SourcePath("where") + "table($table)" to table), SourcePath(""))
        generator2.value(condition)
        machine.delete()
        println(machine.sql.describe())
        val statement = machine.sql.prepare(connection)
        statement.executeUpdate()
    }
}

fun <T: DbSource> db(creator: ((TableConfigure.()->Unit)-> SetRef)->T): DbTableDescription<T> {
    return DbTableDescription(creator)
}*/