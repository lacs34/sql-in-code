package com.losttemple.sql.language.operator

import com.losttemple.sql.language.generate.CountIdGenerator
import com.losttemple.sql.language.generate.DefaultDeleteConstructor
import com.losttemple.sql.language.generate.DefaultUpdateConstructor
import com.losttemple.sql.language.generate.EvaluateContext
import com.losttemple.sql.language.operator.sources.SourceColumn
import com.losttemple.sql.language.types.SqlType
import java.sql.Connection

class DbDeleteEnvironment(table: String, val condition: SqlType<Boolean>?) {
    private val delete = DefaultDeleteConstructor(table)

    init {
        condition?.push(delete.where)
    }

    fun execute(machine: SqlDialect, connection: Connection): Int {
        val context = EvaluateContext(machine, CountIdGenerator(), mapOf(), mapOf())
        delete.root(context)
        println(machine.sql.describe())
        val statement = machine.sql.prepare(connection)
        return statement.executeUpdate()
    }
}