/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.examples.visualization

import scala.util.Random

import org.apache.commons.lang3.RandomStringUtils

import org.apache.spark.examples.visualization.PolytheisticForest.Cookie
import org.apache.spark.sql.SparkSession

// scalastyle:off println
object PolytheisticForest {
  case class Cookie(
    name: String = RandomStringUtils.randomAlphabetic(2),
    value: Double = math.random,
    quality: Double = math.random) {
    def ugly: Boolean = quality < 0.5
    def looksGood(): Option[Cookie] = if (quality > 0.9) None else Some(this)
    def shake(): (Cookie, List[Particle]) =
      Cookie(name, math.max(0, value - 0.1), math.max(0, value - 0.2)) ->
        (1 to Random.nextInt(100)).map(_ => Particle()).toList
  }

  case class Particle(size: Double = math.random)

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("PolytheisticForest")
      .getOrCreate()

    val fromGranny = spark.sparkContext.makeRDD(1 to 1000 map {
      _ => Cookie()
    }, 100).flatMap(_.looksGood()).cache()

    val fromMother = spark.sparkContext.makeRDD(1 to 500 map {
      _ => Cookie()
    }, 50).flatMap(_.looksGood())

    val fromStore = spark.sparkContext.makeRDD(1 to 200 map {
      _ => Cookie()
    }, 500).filter {
      _.ugly
    }

    val cookies = fromGranny
      .repartition(200)
      .union(fromMother.repartition(150))
      .union(fromStore.repartition(500))
      .map(_.shake())
      .cache()

    val remaining = cookies.map(_._1).filter(_.quality != 0).filter(_.value != 0)

    remaining
      .union(fromGranny.repartition(50))
      .groupBy(_.name)
      .map {
        partition =>
          partition._1 -> partition._2.map(_.value).sum
      }
      .join(
        fromGranny.repartition(90).groupBy(_.name)
      )
      .count()

    Thread.currentThread().join()
  }
}

// scalastyle:on println

