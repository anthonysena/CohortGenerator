# Copyright 2021 Observational Health Data Sciences and Informatics
#
# This file is part of CohortGenerator
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Create cohort table(s)
#'
#' @description
#' This function creates an empty cohort table. Optionally, additional empty tables are created to
#' store statistics on the various inclusion criteria.
#'
#' @template Connection
#'
#' @template CohortTable
#'
#' @param createInclusionStatsTables   Create the four additional tables for storing inclusion rule
#'                                     statistics?
#' @param resultsDatabaseSchema        Schema name where the statistics tables reside. Note that for
#'                                     SQL Server, this should include both the database and schema
#'                                     name, for example 'scratch.dbo'.
#' @param cohortInclusionTable         Name of the inclusion table, one of the tables for storing
#'                                     inclusion rule statistics.
#' @param cohortInclusionResultTable   Name of the inclusion result table, one of the tables for
#'                                     storing inclusion rule statistics.
#' @param cohortInclusionStatsTable    Name of the inclusion stats table, one of the tables for storing
#'                                     inclusion rule statistics.
#' @param cohortSummaryStatsTable      Name of the summary stats table, one of the tables for storing
#'                                     inclusion rule statistics.
#'
#' @export
createCohortTable <- function(connectionDetails = NULL,
                              connection = NULL,
                              cohortDatabaseSchema,
                              cohortTable = "cohort",
                              createInclusionStatsTables = FALSE,
                              resultsDatabaseSchema = cohortDatabaseSchema,
                              cohortInclusionTable = paste0(cohortTable, "_inclusion"),
                              cohortInclusionResultTable = paste0(cohortTable, "_inclusion_result"),
                              cohortInclusionStatsTable = paste0(cohortTable, "_inclusion_stats"),
                              cohortSummaryStatsTable = paste0(cohortTable, "_summary_stats")) {
  start <- Sys.time()
  ParallelLogger::logInfo("Creating cohort table")
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  sql <- SqlRender::loadRenderTranslateSql("CreateCohortTable.sql",
                                           packageName = "CohortGenerator",
                                           dbms = connection@dbms,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_table = cohortTable)
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  ParallelLogger::logDebug("- Created table ", cohortDatabaseSchema, ".", cohortTable)

  if (createInclusionStatsTables) {
    ParallelLogger::logInfo("Creating inclusion rule statistics tables")
    sql <- SqlRender::loadRenderTranslateSql("CreateInclusionStatsTables.sql",
                                             packageName = "CohortGenerator",
                                             dbms = connectionDetails$dbms,
                                             cohort_database_schema = resultsDatabaseSchema,
                                             cohort_inclusion_table = cohortInclusionTable,
                                             cohort_inclusion_result_table = cohortInclusionResultTable,
                                             cohort_inclusion_stats_table = cohortInclusionStatsTable,
                                             cohort_summary_stats_table = cohortSummaryStatsTable)
    DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
    ParallelLogger::logDebug("- Created table ", cohortDatabaseSchema, ".", cohortInclusionTable)
    ParallelLogger::logDebug("- Created table ",
                             cohortDatabaseSchema,
                             ".",
                             cohortInclusionResultTable)
    ParallelLogger::logDebug("- Created table ",
                             cohortDatabaseSchema,
                             ".",
                             cohortInclusionStatsTable)
    ParallelLogger::logDebug("- Created table ", cohortDatabaseSchema, ".", cohortSummaryStatsTable)
  }
  delta <- Sys.time() - start
  writeLines(paste("Creating cohort table took", round(delta, 2), attr(delta, "units")))
}



#' Get statistics on cohort inclusion criteria
#'
#' @template Connection
#'
#' @param cohortTable                  Name of the cohort table. Used only to conveniently derive names
#'                                     of the four rule statistics tables.
#' @param cohortId                     The cohort definition ID used to reference the cohort in the
#'                                     cohort table.
#' @param simplify                     Simply output the attrition table?
#' @param resultsDatabaseSchema        Schema name where the statistics tables reside. Note that for
#'                                     SQL Server, this should include both the database and schema
#'                                     name, for example 'scratch.dbo'.
#' @param cohortInclusionTable         Name of the inclusion table, one of the tables for storing
#'                                     inclusion rule statistics.
#' @param cohortInclusionResultTable   Name of the inclusion result table, one of the tables for
#'                                     storing inclusion rule statistics.
#' @param cohortInclusionStatsTable    Name of the inclusion stats table, one of the tables for storing
#'                                     inclusion rule statistics.
#' @param cohortSummaryStatsTable      Name of the summary stats table, one of the tables for storing
#'                                     inclusion rule statistics.
#'
#' @return
#' If `simplify = TRUE`, this function returns a single data frame. Else a list of data frames is
#' returned.
#'
#' @export
getInclusionStatistics <- function(connectionDetails = NULL,
                                   connection = NULL,
                                   resultsDatabaseSchema,
                                   cohortId,
                                   simplify = TRUE,
                                   cohortTable = "cohort",
                                   cohortInclusionTable = paste0(cohortTable, "_inclusion"),
                                   cohortInclusionResultTable = paste0(cohortTable,
                                                                       "_inclusion_result"),
                                   cohortInclusionStatsTable = paste0(cohortTable,
                                                                      "_inclusion_stats"),
                                   cohortSummaryStatsTable = paste0(cohortTable,
                                                                    "_summary_stats")) {
  start <- Sys.time()
  ParallelLogger::logInfo("Fetching inclusion statistics for cohort with cohort_definition_id = ",
                          cohortId)
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  fetchStats <- function(table) {
    ParallelLogger::logDebug("- Fetching data from ", table)
    sql <- "SELECT * FROM @database_schema.@table WHERE cohort_definition_id = @cohort_id"
    DatabaseConnector::renderTranslateQuerySql(sql = sql,
                                               connection = connection,
                                               snakeCaseToCamelCase = TRUE,
                                               database_schema = resultsDatabaseSchema,
                                               table = table,
                                               cohort_id = cohortId)
  }
  inclusion <- fetchStats(cohortInclusionTable)
  summaryStats <- fetchStats(cohortSummaryStatsTable)
  inclusionStats <- fetchStats(cohortInclusionStatsTable)
  inclusionResults <- fetchStats(cohortInclusionResultTable)
  result <- processInclusionStats(inclusion = inclusion,
                                  inclusionResults = inclusionResults,
                                  inclusionStats = inclusionStats,
                                  summaryStats = summaryStats,
                                  simplify = simplify)
  delta <- Sys.time() - start
  writeLines(paste("Fetching inclusion statistics took", round(delta, 2), attr(delta, "units")))
  return(result)
}


processInclusionStats <- function(inclusion,
                                  inclusionResults,
                                  inclusionStats,
                                  summaryStats,
                                  simplify) {
  if (simplify) {
    if (nrow(inclusion) == 0 || nrow(inclusionStats) == 0) {
      return(data.frame())
    }
    result <- merge(unique(inclusion[, c("ruleSequence", "name")]),
                    inclusionStats[inclusionStats$modeId ==
      0, c("ruleSequence", "personCount", "gainCount", "personTotal")], )

    result$remain <- rep(0, nrow(result))
    inclusionResults <- inclusionResults[inclusionResults$modeId == 0, ]
    mask <- 0
    for (ruleId in 0:(nrow(result) - 1)) {
      mask <- bitwOr(mask, 2^ruleId)
      idx <- bitwAnd(inclusionResults$inclusionRuleMask, mask) == mask
      result$remain[result$ruleSequence == ruleId] <- sum(inclusionResults$personCount[idx])
    }
    colnames(result) <- c("ruleSequenceId",
                          "ruleName",
                          "meetSubjects",
                          "gainSubjects",
                          "totalSubjects",
                          "remainSubjects")
  } else {
    if (nrow(inclusion) == 0) {
      return(list())
    }
    result <- list(inclusion = inclusion,
                   inclusionResults = inclusionResults,
                   inclusionStats = inclusionStats,
                   summaryStats = summaryStats)
  }
  return(result)
}

#' Instantiate a set of cohort
#'
#' @description
#' This function instantiates a set of cohort in the cohort table, using definitions that are fetched
#' from a WebApi interface. Optionally, the inclusion rule statistics are computed and stored in the
#' \code{inclusionStatisticsFolder}.
#'
#' @template Connection
#'
#' @param numThreads                  Specify the number of threads for cohort generation. Currently
#'                                    this only supports single threaded operations.
#'
#' @template CohortTable
#'
#' @template OracleTempSchema
#'
#' @template CdmDatabaseSchema
#'
#' @template CohortSetSpecs
#'
#' @template CohortSetReference
#'
#' @param cohorts                     A data.frame containing the cohort set to instantiate. This data
#'                                    frame must include the following columns: cohortId, cohortFullName,
#'                                    sql and json. The json column must represent a circe cohort definition
#'                                    construction to.
#' @param generateInclusionStats      Compute and store inclusion rule statistics?
#' @param inclusionStatisticsFolder   The folder where the inclusion rule statistics are stored. Can be
#'                                    left NULL if \code{generateInclusionStats = FALSE}.
#' @param createCohortTable           Create the cohort table? If \code{incremental = TRUE} and the
#'                                    table already exists this will be skipped.
#' @param incremental                 Create only cohorts that haven't been created before?
#' @param incrementalFolder           If \code{incremental = TRUE}, specify a folder where records are
#'                                    kept of which definition has been executed.
#'
#' @export
instantiateCohortSet <- function(connectionDetails = NULL,
                                 connection = NULL,
                                 numThreads = 1,
                                 cdmDatabaseSchema,
                                 oracleTempSchema = NULL,
                                 cohortDatabaseSchema = cdmDatabaseSchema,
                                 cohortTable = "cohort",
                                 cohorts = NULL,
                                 generateInclusionStats = FALSE,
                                 inclusionStatisticsFolder = NULL,
                                 createCohortTable = FALSE,
                                 incremental = FALSE,
                                 incrementalFolder = NULL) {
  if (!is.null(cohorts) & is.data.frame(cohorts)) {
    cohortRequiredColumns <- c("cohortId", "cohortFullName", "sql", "json")
    if (length(intersect(names(cohorts), cohortRequiredColumns)) != length(cohortRequiredColumns)) {
      stop(paste("The cohorts data frame must contain the following columns:", cohortRequiredColumns, sep = ","))
    }
  } else {
    stop("The cohorts parameter is mandatory and must be a data frame.")
  }
  if (generateInclusionStats) {
    if (is.null(inclusionStatisticsFolder)) {
      stop("Must specify inclusionStatisticsFolder when generateInclusionStats = TRUE")
    }
    if (!file.exists(inclusionStatisticsFolder)) {
      dir.create(inclusionStatisticsFolder, recursive = TRUE)
    }
  }
  if (incremental) {
    if (is.null(incrementalFolder)) {
      stop("Must specify incrementalFolder when incremental = TRUE")
    }
    if (!file.exists(incrementalFolder)) {
      dir.create(incrementalFolder, recursive = TRUE)
    }
  }
  
  # This is for when we parallel cohort generation
  #if (numThreads < 1 || numThreads > parallel::detectCores()) {
  #  stop(paste0("The numThreads argument must be between 1 and", parallel::detectCores()))
  #}
  if (numThreads != 1) {
    stop("")
  }

  start <- Sys.time()
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  if (createCohortTable) {
    needToCreate <- TRUE
    if (incremental) {
      tables <- DatabaseConnector::getTableNames(connection, cohortDatabaseSchema)
      if (toupper(cohortTable) %in% toupper(tables)) {
        ParallelLogger::logInfo("Cohort table already exists and in incremental mode, so not recreating table.")
        needToCreate <- FALSE
      }
    }
    if (needToCreate) {
      createCohortTable(connection = connection,
                        cohortDatabaseSchema = cohortDatabaseSchema,
                        cohortTable = cohortTable,
                        createInclusionStatsTables = FALSE)
    }
  }

  if (incremental) {
    cohorts$checksum <- computeChecksum(cohorts$sql)
    recordKeepingFile <- file.path(incrementalFolder, "InstantiatedCohorts.csv")
  }

  if (numThreads > 1) {
    cluster <- ParallelLogger::logInfo(paste0("Generating cohorts in parallel using ",
                                              numThreads,
                                              " threads. Individual cohort generation progress will not be displayed in the console."))
  }

  # Create the cluster
  cluster <- ParallelLogger::makeCluster(numberOfThreads = numThreads)
  on.exit(ParallelLogger::stopCluster(cluster), add = TRUE)

  # Apply the generation operation to the cluster
  cohortsGenerated <- ParallelLogger::clusterApply(cluster,
                                                   cohorts$cohortId,
                                                   generateCohort,
                                                   cohorts = cohorts,
                                                   connectionDetails = connectionDetails,
                                                   cdmDatabaseSchema = cdmDatabaseSchema,
                                                   oracleTempSchema = oracleTempSchema,
                                                   cohortDatabaseSchema = cohortDatabaseSchema,
                                                   cohortTable = cohortTable,
                                                   generateInclusionStats = generateInclusionStats,
                                                   inclusionStatisticsFolder = inclusionStatisticsFolder,
                                                   incremental = incremental,
                                                   recordKeepingFile = recordKeepingFile,
                                                   stopOnError = TRUE,
                                                   progressBar = TRUE)

  delta <- Sys.time() - start
  writeLines(paste("Instantiating cohort set took", round(delta, 2), attr(delta, "units")))
  return(cohortsGenerated)
}

#' @export
generateCohort <- function(cohortId = NULL,
                           cohorts,
                           connectionDetails = NULL,
                           cdmDatabaseSchema,
                           oracleTempSchema,
                           cohortDatabaseSchema,
                           cohortTable,
                           generateInclusionStats,
                           inclusionStatisticsFolder,
                           incremental,
                           recordKeepingFile) {

  # Get the index of the cohort record for the current cohortId
  i <- which(cohorts$cohortId == cohortId)
  if (!incremental || isTaskRequired(cohortId = cohorts$cohortId[i],
                                     checksum = cohorts$checksum[i],
                                     recordKeepingFile = recordKeepingFile)) {
    # Establish the connection and ensure the cleanup is performed
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))

    if (generateInclusionStats) {
      createTempInclusionStatsTables(connection, oracleTempSchema, cohorts)
    }

    # Log the operation
    ParallelLogger::logInfo(i,
                            "/",
                            nrow(cohorts),
                            ": Instantiation cohort ",
                            cohorts$cohortFullName[i])
    sql <- cohorts$sql[i]
    .warnMismatchSqlInclusionStats(sql, generateInclusionStats = generateInclusionStats)
    if (generateInclusionStats) {
      sql <- SqlRender::render(sql,
                               cdm_database_schema = cdmDatabaseSchema,
                               vocabulary_database_schema = cdmDatabaseSchema,
                               target_database_schema = cohortDatabaseSchema,
                               target_cohort_table = cohortTable,
                               target_cohort_id = cohorts$cohortId[i],
                               results_database_schema.cohort_inclusion = "#cohort_inclusion",
                               results_database_schema.cohort_inclusion_result = "#cohort_inc_result",
                               results_database_schema.cohort_inclusion_stats = "#cohort_inc_stats",
                               results_database_schema.cohort_summary_stats = "#cohort_summary_stats")
    } else {
      sql <- SqlRender::render(sql,
                               cdm_database_schema = cdmDatabaseSchema,
                               vocabulary_database_schema = cdmDatabaseSchema,
                               target_database_schema = cohortDatabaseSchema,
                               target_cohort_table = cohortTable,
                               target_cohort_id = cohorts$cohortId[i])
    }
    sql <- SqlRender::translate(sql,
                                targetDialect = connectionDetails$dbms,
                                oracleTempSchema = oracleTempSchema)
    DatabaseConnector::executeSql(connection, sql)

    if (generateInclusionStats) {
      saveAndDropTempInclusionStatsTables(connection = connection,
                                          oracleTempSchema = oracleTempSchema,
                                          inclusionStatisticsFolder = inclusionStatisticsFolder,
                                          incremental = incremental,
                                          cohortIds = cohorts$cohortId[i])
    }

    if (incremental) {
      recordTasksDone(cohortId = cohorts$cohortId[i],
                      checksum = cohorts$checksum[i],
                      recordKeepingFile = recordKeepingFile)
    }

    return(cohorts$cohortId[i])
  } else {
    return(NULL)
  }
}

createTempInclusionStatsTables <- function(connection, oracleTempSchema, cohorts) {
  ParallelLogger::logInfo("Creating temporary inclusion statistics tables")
  sql <- SqlRender::loadRenderTranslateSql("inclusionStatsTables.sql",
                                           packageName = "CohortGenerator",
                                           dbms = connection@dbms,
                                           oracleTempSchema = oracleTempSchema)
  DatabaseConnector::executeSql(connection, sql)

  inclusionRules <- data.frame(cohortDefinitionId = as.double(),
                               ruleSequence = as.integer(),
                               name = as.character())
  for (i in 1:nrow(cohorts)) {
    cohortDefinition <- RJSONIO::fromJSON(content = cohorts$json[i], digits = 23)
    if (!is.null(cohortDefinition$InclusionRules)) {
      nrOfRules <- length(cohortDefinition$InclusionRules)
      if (nrOfRules > 0) {
        for (j in 1:nrOfRules) {
          inclusionRules <- rbind(inclusionRules, data.frame(cohortDefinitionId = cohorts$cohortId[i],
                                                             ruleSequence = j - 1,
                                                             name = cohortDefinition$InclusionRules[[j]]$name))
        }
      }
    }
  }

  DatabaseConnector::insertTable(connection = connection,
                                 tableName = "#cohort_inclusion",
                                 data = inclusionRules,
                                 dropTableIfExists = TRUE,
                                 createTable = TRUE,
                                 tempTable = TRUE,
                                 oracleTempSchema = oracleTempSchema,
                                 camelCaseToSnakeCase = TRUE)
}

saveAndDropTempInclusionStatsTables <- function(connection,
                                                oracleTempSchema,
                                                inclusionStatisticsFolder,
                                                incremental,
                                                cohortIds) {
  fetchStats <- function(table, fileName, cohortIds) {
    ParallelLogger::logDebug("- Fetching data from ", table)
    sql <- "SELECT * FROM @table"
    data <- DatabaseConnector::renderTranslateQuerySql(sql = sql,
                                                       connection = connection,
                                                       oracleTempSchema = oracleTempSchema,
                                                       snakeCaseToCamelCase = TRUE,
                                                       table = table)
    fullFileName <- file.path(inclusionStatisticsFolder, fileName)
    if(nrow(data) > 0) {
      if (incremental) {
        saveIncremental(data, fullFileName, cohortDefinitionId = cohortIds)
      } else {
        readr::write_csv(x = data, file = fullFileName)
      }
    }
  }
  fetchStats("#cohort_inclusion", "cohortInclusion.csv", cohortIds)
  fetchStats("#cohort_inc_result", "cohortIncResult.csv", cohortIds)
  fetchStats("#cohort_inc_stats", "cohortIncStats.csv", cohortIds)
  fetchStats("#cohort_summary_stats", "cohortSummaryStats.csv", cohortIds)

  sql <- "TRUNCATE TABLE #cohort_inclusion;
  DROP TABLE #cohort_inclusion;

  TRUNCATE TABLE #cohort_inc_result;
  DROP TABLE #cohort_inc_result;

  TRUNCATE TABLE #cohort_inc_stats;
  DROP TABLE #cohort_inc_stats;

  TRUNCATE TABLE #cohort_summary_stats;
  DROP TABLE #cohort_summary_stats;"
  DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                               sql = sql,
                                               progressBar = FALSE,
                                               reportOverallTime = FALSE,
                                               oracleTempSchema = oracleTempSchema)
}

.warnMismatchSqlInclusionStats <- function(sql, generateInclusionStats) {
  if (any(stringr::str_detect(string = sql, pattern = "_inclusion_result"),
          stringr::str_detect(string = sql, pattern = "_inclusion_stats"),
          stringr::str_detect(string = sql, pattern = "_summary_stats"))) {
    if (isFALSE(generateInclusionStats)) {
      warning("The SQL template used to instantiate cohort was designed to output cohort inclusion statistics.
        But, generateInclusionStats is set to False while instantiating cohort.
        This may cause error and terminate cohort generation.")
    }
  } else {
    if (isTRUE(generateInclusionStats)) {
      warning("The SQL template used to instantiate cohort was designed to NOT output cohort inclusion statistics.
        But, generateInclusionStats is set to TRUE while instantiating cohort.
        This may cause error and terminate cohort generation.")
    }
  }
}
