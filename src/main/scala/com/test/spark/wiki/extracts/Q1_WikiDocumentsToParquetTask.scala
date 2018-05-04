package com.test.spark.wiki.extracts

import java.net.URL

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.DateTime
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.Seq
import scala.io.Source



// TODO Q8 Ajouter les annotations manquantes pour pouvoir mapper le fichier yaml à cette classe
case class LeagueInput(@JsonProperty(value ="name", required = true) name: String,
                       @JsonProperty(value="url", required = true) url: String ){
 def this ()= this("","")
}

case class LeagueStanding(league: String,
                          season: Int,
                          position: Int,
                          team: String,
                          points: Int,
                          played: Int,
                          won: Int,
                          drawn: Int,
                          lost: Int,
                          goalsFor: Int,
                          goalsAgainst: Int,
                          goalsDifference: Int)



case class Q1_WikiDocumentsToParquetTask(bucket: String) extends Runnable {
  private val session: SparkSession = SparkSession.builder().appName("test").master("local[*]").
    getOrCreate()

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  import session.implicits._


  override def run(): Unit = {
    val toDate = DateTime.now().withYear(2017)
    val fromDate = toDate.minusYears(40)

    session.createDataset(getLeagues)
      //  .toDS() // TODO Q1 Transformer cette seq en dataset
      .flatMap {
      input =>
        (fromDate.getYear until toDate.getYear).map {
          year =>
            year + 1 -> (input.name, input.url.format(year, year + 1))
        }
    }.flatMap {
      case (season, (league, url)) =>
        try {// TODO Q2 Implémenter le parsing Jsoup. Il faut retourner une Seq[LeagueStanding]
          // ATTENTION:
          //  - Quelques pages ne respectent pas le format commun et pourraient planter - pas grave
          //  - Il faut veillez à recuperer uniquement le classement général
          //  - Il faut normaliser les colonnes "positions", "teams" et "points" en cas de problèmes de formatage

          val doc:Document = Jsoup.connect(url).get
          val content:Element = doc.getElementById("Général")

          Seq[LeagueStanding]() :+ (LeagueStanding(league, season, content.getElementById("Rang").data().toInt,
            content.getElementById("Equipe").data(), content.getElementById("Points").data().toInt,
            content.getElementById("Matchs joués").data().toInt, content.getElementById("Matchs gagnés").data().toInt,
            content.getElementById("Matchs nuls").data().toInt, content.getElementById("Matchs perdus").data().toInt,
            content.getElementById("Buts pour").data().toInt, content.getElementById("Buts Contre").data().toInt,
            content.getElementById("Difference de buts").data().toInt))

        } catch {
          case _: Throwable =>
            // TODO Q3 En supposant que ce job tourne sur un cluster EMR, où seront affichés les logs d'erreurs ?
            logger.warn(s"Can't parse season $season from $url")
            Seq.empty
        }
    }.repartition(2) // TODO Q4 Comment partitionner les données en 2 avant l'écriture sur le bucket
      .write
      .mode(SaveMode.Overwrite)
      .parquet(bucket)

    // TODO Q5 Quel est l'avantage du format parquet par rapport aux autres formats ?
    // Format colonne, bonne perf pour le requêtage, supporte les structure de données complexe, peut être utilisé par,
    // beaucoup d'interface

    // TODO Q6 Quel est l'avantage de passer d'une séquence scala à un dataset spark ?
    // Permet de repérer les erreurs dès la compiltation, fortement typé, permet d'utiliser au mieux l'optimisateur de
    //requêtes Catalyst

  }

  private def getLeagues: Seq[LeagueInput] =
  {
    val mapper = new ObjectMapper(new YAMLFactory())
    // TODO Q7 Recuperer l'input stream du fichier leagues.yaml
    val inputStream=Source.fromInputStream(this.getClass.getClassLoader.getResourceAsStream("leagues.yaml")).reader()
    mapper.readValue(inputStream, classOf[Array[LeagueInput]]).toSeq
  }
}

