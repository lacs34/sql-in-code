package com.losttemple.sql.language.generate

class CountIdGenerator: IdGenerator {
    private var current = 0

    override fun nextId(): String {
        val id = "id$current"
        current++
        return id
    }
}