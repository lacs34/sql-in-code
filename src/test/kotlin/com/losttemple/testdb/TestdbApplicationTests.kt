package com.losttemple.testdb

import com.losttemple.sql.language.dialects.H2Environment
import com.losttemple.sql.language.operator.*
import com.losttemple.sql.language.reflection.useAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.*
import kotlin.collections.ArrayList

class QueryTests {
	companion object {
	    private lateinit var h2: H2Environment

		@BeforeAll
		@JvmStatic
		fun setUpBeforeClass() {
			try {
				h2 = H2Environment("test")
				h2.runSql("CREATE TABLE `rooms` (\n" +
						"  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
						"  `apartment` bigint DEFAULT NULL,\n" +
						"  `type` int DEFAULT '0',\n" +
						"  `floor` int DEFAULT '1',\n" +
						"  `sold` tinyint(1) DEFAULT '0',\n" +
						"  `sold_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
						"  `capacity` int DEFAULT NULL,\n" +
						"  PRIMARY KEY (`id`)\n" +
						")")
				h2.runSql("CREATE TABLE `apartments` (\n" +
						"  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
						"  `address` varchar(255) NOT NULL,\n" +
						"  `lat` double NOT NULL,\n" +
						"  `lon` double NOT NULL,\n" +
						"  `build_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
						"  `maintenance_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
						"  `buildup` tinyint(1) NOT NULL DEFAULT '0',\n" +
						"  `min_floor` int NOT NULL,\n" +
						"  `max_floor` int NOT NULL,\n" +
						"  PRIMARY KEY (`id`)\n" +
						")")
				h2.runSql("INSERT INTO `rooms` VALUES\n" +
						"(1,1,0,2,0,'2017-01-05 13:24:52',20),\n" +
						"(2,1,0,3,0,'2017-08-12 17:02:33',20),\n" +
						"(3,1,0,4,0,'2017-12-04 09:47:12',20),\n" +
						"(4,1,0,5,0,'2018-04-30 15:04:55',20),\n" +
						"(5,1,0,7,0,'2020-02-04 17:19:49',20)")
				h2.runSql("INSERT INTO `apartments` VALUES\n" +
						"(1,'园山街道保安社区简一村龙腾街25-1',22.51124,114.05122,'2017-01-01 0:0:0','2017-01-01 0:0:0',1,1,8),\n" +
						"(2,'松岗街道东方社区田洋二路1号B801B区',22.51233,114.05302,'2017-02-05 0:0:0','2017-07-05 0:0:0',0,1,20)")
			}
			catch (ex: Throwable) {
				ex.printStackTrace()
				throw ex
			}
		}
	}

	private fun <L, R> assertListItemEqual(leftItem: L, rightIndices: List<Int>, right: List<R>, equal: (L, R) -> Boolean): Int {
		for (rightIndex in rightIndices) {
			val rightItem = right[rightIndex]
			if (equal(leftItem, rightItem)) {
				return rightIndex
			}
		}
		return -1
	}

	private fun <L, R> assertListNoorderEqual(left: List<L>, right: List<R>, equal: (L, R) -> Boolean) {
		assertEquals(left.size, right.size)
		val notComparedIndices = ArrayList<Int>(right.size).apply {
			addAll(right.indices)
		}
		for (leftItem in left) {
			val matchIndex = assertListItemEqual(leftItem, notComparedIndices, right, equal)
			if (matchIndex >= 0) {
				notComparedIndices.remove(matchIndex)
			}
			else {
				assert(false)
			}
		}
	}

	@Test
	fun basicQuery() {
		 val result = h2.run {
			from { Apartments(it) }.use {
				need(it.id)
				need(it.address)
				need(it.lat)
				need(it.lon)
			}.select {
				object {
					val id = it.id()
					val address = it.address()
					val lat = it.lat()
					val lon = it.lon()
				}
			}
		}
		assertListNoorderEqual(result, apartmentsData) { left, right ->
			(left.id == right.id) &&
			(left.lat == right.lat) &&
			(left.lon == right.lon) &&
			(left.address == right.address)
		}
	}

	@Test
	fun groupRemoveOrder() {
		val result = h2.run {
			from { Rooms(it) }.order { id }.group { floor }.select {
				object {
					val floor = key()
					val allSold = min{sold}
				}
			}.useAll().select {
				object {
					val floor = it.floor()
					val allSold = it.allSold()
				}
			}
		}
		assert(true)
	}

	@Test
	fun basicInsert() {
		val result = h2.run {
			db { Rooms(it) }.insert {
				it.floor(3)
				it.soldTime(Date())
			}()
		}
	}

	@Test
	fun basicUpdate() {
		val result = h2.run {
			db { Rooms(it) }.update {
				//it.floor(it.capacity)
				it.soldTime(it.soldTime + Duration.ofHours(2))
			}()
		}
	}

	@Test
	fun basicUpdateWithFilter() {
		val result = h2.run {
			(db { Rooms(it) } where {
				floor eq 2
			} where {
				capacity gt floor
			}).update {
				it.floor(it.capacity)
				it.soldTime(it.soldTime + Duration.ofHours(2))
			}()
		}
	}
}
