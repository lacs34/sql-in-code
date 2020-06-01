package com.losttemple.sql.language.generate

import java.math.BigInteger
import java.sql.Date
import java.sql.Time
import java.sql.Timestamp
import java.time.Duration

enum class GroupStatus {
    None,
    Group,
    Aggregate
}

interface ReferenceConstructor {
    fun joinLeft(): ReferenceConstructor
    fun joinRight(): ReferenceConstructor
    fun group(): ReferenceConstructor
    fun column(name: String)
}

enum class OrderStatus {
    None,
    Ascending,
    Descending
}

enum class LimitStatus {
    None,
    Limit,
    LimitWithOffset
}

interface SourceReference {
    val from: SourceConstructor
    val where: ExpressionConstructor
    val whereAlwaysTrue: Boolean
    val having: ExpressionConstructor
    val havingAlwaysTrue: Boolean
    val offset: Int
    var limit: Int
    val limitStatus: LimitStatus
    fun limitWithOffset(limit: Int, offset: Int)
    val group: ExpressionConstructor
    fun aggregate()
    val groupStatus: GroupStatus
    fun turnToEmbed(): QueryConstructor
    val order: OrderExpression
    val hasOrder: Boolean
}

interface ContractExpression: ExpressionConstructor {
    fun commit()
}

interface ExpressionConstructor {
    fun constance(value: BigInteger?)
    fun constance(value: String?)
    fun constance(value: Date?)
    fun constance(value: Time?)
    fun constance(value: Timestamp?)
    fun constance(value: Boolean?)
    fun constance(value: Double?)
    fun and()
    fun or()
    fun eq()
    fun greater()
    fun add()
    fun subtraction()
    fun addPeriod(period: Duration)
    fun subPeriod(period: Duration)
    fun max()
    fun min()
    fun sum()
    fun count()
    fun now()
    fun reference(sourceHash: (SourceReference) -> Unit, constructor: (ReferenceConstructor) -> Unit): Boolean
    fun pushDown(direction: (SourceReference) -> Unit, destination: (ReferenceConstructor) -> Unit)
    fun embedQuery(): QueryConstructor

    fun contract(): ContractExpression
}

interface SourceConstructor {
    fun table(name: String)
    fun leftJoin(): ExpressionConstructor
    fun rightJoin(): ExpressionConstructor
    fun innerJoin(): ExpressionConstructor
    fun outerJoin(): ExpressionConstructor
    fun embedQuery(): QueryConstructor
    fun turnToEmbed(): QueryConstructor
}

fun ExpressionConstructor.withContract(builder: ContractExpression.() -> Unit) {
    val contract = contract()
    contract.builder()
    contract.commit()
}

fun OrderExpression.withContract(builder: ContractOrderExpression.() -> Unit) {
    val contract = contract()
    contract.builder()
    contract.commit()
}