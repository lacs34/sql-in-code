package com.losttemple.sql.language.operator

/*
class DbUpdateEnvironment(private val table: String, private val filter: SqlType<Boolean>?) {
    private data class ColumnValue(
            val column: String,
            val value: SqlParameter
    )
    private val columnValues = ArrayList<ColumnValue>()

    operator fun <T> SourceColumn<T>.invoke(value: T) {
        columnValues.add(ColumnValue(name, generateParameter(value)))
    }

    operator fun <T> SourceColumn<T>.invoke(value: SqlType<T>) {
        columnValues.add(ColumnValue(name, SqlTypeParameter(value)))
    }

    fun execute(machine: SqlDialect, connection: Connection) {
        val generator = SqlGenerator(machine)
        var first = true
        machine.table(table)
        machine.columnList()
        val columnValueContext = if (filter == null) {
            ValueGenerateContext(mapOf(SourcePath("table($table)") to table), SourcePath(""))
        }
        else {
            ValueGenerateContext(mapOf(SourcePath("where") + "table($table)" to table), SourcePath(""))
        }
        for (column in columnValues) {
            machine.column(column.column)
            column.value.setParam(generator, columnValueContext)
            machine.assign()
            if (!first) {
                machine.addAssign()
            }
            first = false
        }
        if (filter != null) {
            val generator2 = generator.withSource(mapOf(SourcePath("where") + "table($table)" to table), SourcePath(""))
            generator2.value(filter)
            machine.updateWithFilter()
        }
        else {
            machine.updateAll()
        }

        println(machine.sql.describe())
        val statement = machine.sql.prepare(connection)
        statement.executeUpdate()
    }
}*/