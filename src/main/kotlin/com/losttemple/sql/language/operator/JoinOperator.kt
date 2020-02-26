package com.losttemple.sql.language.operator

import com.losttemple.sql.language.generate.ExpressionConstructor
import com.losttemple.sql.language.generate.GroupStatus
import com.losttemple.sql.language.generate.OrderStatus
import com.losttemple.sql.language.generate.SourceReference
import com.losttemple.sql.language.types.*


class JoinSource<S1, S2>(val left: DbSet<S1>, val right: DbSet<S2>, val type: String) {
}

infix fun <S1, S2> DbSet<S1>.leftJoin(s2: DbSet<S2>): JoinSource<S1, S2> {
    return JoinSource(this, s2, "LEFT")
}

infix fun <S1, S2> DbSet<S1>.rightJoin(s2: DbSet<S2>): JoinSource<S1, S2> {
    return JoinSource(this, s2, "RIGHT")
}

infix fun <S1, S2> DbSet<S1>.innerJoin(s2: DbSet<S2>): JoinSource<S1, S2> {
    return JoinSource(this, s2, "INNER")
}

infix fun <S1, S2> DbSet<S1>.join(s2: DbSet<S2>): JoinSource<S1, S2> {
    return JoinSource(this, s2, "INNER")
}

infix fun <S1, S2> DbSet<S1>.outerJoin(s2: DbSet<S2>): JoinSource<S1, S2> {
    return JoinSource(this, s2, "OUTER")
}

infix fun <S1, S2> DbSet<S1>.fullJoin(s2: DbSet<S2>): JoinSource<S1, S2> {
    return JoinSource(this, s2, "OUTER")
}

infix fun <S1, S2> DbSet<S1>.fullOuterJoin(s2: DbSet<S2>): JoinSource<S1, S2> {
    return JoinSource(this, s2, "OUTER")
}

class JoinSet<S1, S2>(
        val leftSet: DbSet<S1>,
        val rightSet: DbSet<S2>,
        val left: SqlSet,
        val right: SqlSet,
        val handler: (s1: S1, s2: S2) -> SqlType<Boolean>,
        val type: String) {
}

class JoinOnlySet(private val left: SqlSet, private val right: SqlSet, private val type: String): SqlSet {
    override fun push(constructor: SourceReference) {
        left.push(constructor)
        if (!constructor.whereAlwaysTrue || constructor.groupStatus != GroupStatus.None ||
                constructor.orderStatus != OrderStatus.None || constructor.hasLimit) {
            constructor.turnToEmbed()
        }
        right.push(constructor)
        if (!constructor.whereAlwaysTrue || constructor.groupStatus != GroupStatus.None ||
                constructor.orderStatus != OrderStatus.None || constructor.hasLimit) {
            constructor.turnToEmbed()
        }
        when (type) {
            "LEFT"-> constructor.from.leftJoin().now()
            "RIGHT"-> constructor.from.rightJoin().now()
            "INNER"-> constructor.from.innerJoin().now()
            "OUTER"-> constructor.from.outerJoin().now()
        }
    }
}

infix fun <S1, S2> JoinSource<S1, S2>.on(handler: (s1: S1, s2: S2) -> SqlType<Boolean>): JoinSet<S1, S2> {
    val leftSet = left.set
    val rightSet = right.set
    return JoinSet(left, right, leftSet, rightSet, handler, type)
}

class JoinResultSet(private val left: SqlSet, private val right: SqlSet, private val type: String): SqlSet {
    lateinit var condition: SqlType<Boolean>

    private fun contractPushCondition(constructor: ExpressionConstructor) {
        val contract = constructor.contract()
        condition.push(contract)
        contract.commit()
    }

    override fun push(constructor: SourceReference) {
        left.push(constructor)
        if (!constructor.whereAlwaysTrue || constructor.groupStatus != GroupStatus.None ||
                constructor.orderStatus != OrderStatus.None || constructor.hasLimit) {
            constructor.turnToEmbed()
        }
        right.push(constructor)
        if (!constructor.whereAlwaysTrue || constructor.groupStatus != GroupStatus.None ||
                constructor.orderStatus != OrderStatus.None || constructor.hasLimit) {
            constructor.turnToEmbed()
        }
        when (type) {
            "LEFT"-> contractPushCondition(constructor.from.leftJoin())
            "RIGHT"-> contractPushCondition(constructor.from.rightJoin())
            "INNER"-> contractPushCondition(constructor.from.innerJoin())
            "OUTER"-> contractPushCondition(constructor.from.outerJoin())
        }
    }
}

class JoinResult<S1, S2, R>(
        private val leftSet: DbSet<S1>,
        private val rightSet: DbSet<S2>,
        left: SqlSet,
        right: SqlSet,
        private val selector: (s1: S1, s2: S2) -> R,
        handler: (s1: S1, s2: S2) -> SqlType<Boolean>,
        type: String
): DbSet<R> {
    override val description: R
        get() = result
    override val set: SqlSet
    private val result: R
    init {
        val resultSet: JoinResultSet = JoinResultSet(left, right, type)
        set = resultSet
        val joinOnlySet = JoinOnlySet(left, right, type)
        val transferredLeft = leftSet.wrapDescription(OperatorPrefix(joinOnlySet, listOf { ir -> ir.joinLeft()}))
        val transferredRight = rightSet.wrapDescription(OperatorPrefix(joinOnlySet, listOf { ir -> ir.joinRight()}))
        resultSet.condition = handler(transferredLeft, transferredRight)
        val resultLeft = leftSet.wrapDescription(OperatorPrefix(resultSet, listOf { ir -> ir.joinLeft()}))
        val resultRight = rightSet.wrapDescription(OperatorPrefix(resultSet, listOf { ir -> ir.joinRight()}))
        result = selector(resultLeft, resultRight)
    }

    override fun wrapDescription(prefix: SetPrefix): R {
        val transferredLeft = leftSet.wrapDescription(prefix.append { it.joinLeft() })
        val transferredRight = rightSet.wrapDescription(prefix.append { it.joinRight() })
        return selector(transferredLeft, transferredRight)
    }
}

infix fun <S1, S2, R> JoinSet<S1, S2>.select(handler: (s1: S1, s2: S2) -> R): DbSet<R> {
    return JoinResult<S1, S2, R>(leftSet, rightSet, left, right, handler, this.handler, type)
}