library(testthat)
library(CohortGenerator)

# Test Prep ----------------
cohortJsonFiles <- list.files(path = system.file("cohorts", package = "CohortGenerator"), full.names = TRUE)
cohorts <- setNames(data.frame(matrix(ncol = 4, nrow = 0), stringsAsFactors = FALSE), c("cohortId","cohortFullName", "sql", "json"))
for (i in 1:length(cohortJsonFiles)) {
  cohortFullName <- tools::file_path_sans_ext(basename(cohortJsonFiles[i]))
  cohortJson <- CohortGenerator::readCirceExpressionJsonFile(cohortJsonFiles[i])
  cohortExpression <- CohortGenerator::createCirceExpressionFromFile(cohortJsonFiles[i])
  cohortSql <- CirceR::buildCohortQuery(cohortExpression, options = CirceR::createGenerateOptions(generateStats = TRUE))
  cohorts <- rbind(cohorts, data.frame(cohortId = i, 
                                       cohortFullName = cohortFullName, 
                                       sql = cohortSql, 
                                       json = cohortJson,
                                       stringsAsFactors = FALSE))
}

# Exception Handling -------------
test_that("Call instantiateCohortSet with default parameters", {
  expect_error(instantiateCohortSet(),
               message = "(cohorts parameter)")
})

test_that("Call instatiateCohortSet with generateInclusionStats = TRUE and no folder specified", {
  expect_error(instantiateCohortSet(generateInclusionStats = TRUE),
               message = "(Must specify inclusionStatisticsFolder)")
})

test_that("Call instatiateCohortSet with incremental = TRUE and no folder specified", {
  expect_error(instantiateCohortSet(incremental = TRUE),
               message = "(Must specify incrementalFolder)")
})

test_that("Call instatiateCohortSet without connection or connectionDetails specified", {
  expect_error(instantiateCohortSet(connectionDetails = NULL,
                                    connection = NULL,
                                    cohorts = cohorts),
               message = "ConnectionDetails missing")
})

# Functional Tests ----------------
connectionDetails <- Eunomia::getEunomiaConnectionDetails()

test_that("Create cohorts - Gen Stats = T, Incremental = F", {
  outputFolder <- tempdir()
  # Run first to ensure that all cohorts are generated
  cohortsGenerated <- instantiateCohortSet(connectionDetails = connectionDetails,
                                           cdmDatabaseSchema = "main",
                                           cohortDatabaseSchema = "main",
                                           cohortTable = "temp_cohort",
                                           cohorts = cohorts,
                                           createCohortTable = TRUE,
                                           generateInclusionStats = TRUE,
                                           incremental = FALSE,
                                           incrementalFolder = file.path(outputFolder, "RecordKeeping"),
                                           inclusionStatisticsFolder = outputFolder)
  expect_equal(length(cohortsGenerated), nrow(cohorts))
  unlink(outputFolder)
})

test_that("Create cohorts - Gen Stats = T, Incremental = T", {
  outputFolder <- tempdir()
  # Run first to ensure that all cohorts are generated
  cohortsGenerated <- instantiateCohortSet(connectionDetails = connectionDetails,
                                           cdmDatabaseSchema = "main",
                                           cohortDatabaseSchema = "main",
                                           cohortTable = "temp_cohort",
                                           cohorts = cohorts,
                                           createCohortTable = TRUE,
                                           generateInclusionStats = TRUE,
                                           incremental = FALSE,
                                           incrementalFolder = file.path(outputFolder, "RecordKeeping"),
                                           inclusionStatisticsFolder = outputFolder)
  # Next run using incremental mode to verify that all cohorts are created
  # but the return indicates that nothing new was generated
  cohortsGenerated <- instantiateCohortSet(connectionDetails = connectionDetails,
                                           cdmDatabaseSchema = "main",
                                           cohortDatabaseSchema = "main",
                                           cohortTable = "temp_cohort",
                                           cohorts = cohorts,
                                           createCohortTable = TRUE,
                                           generateInclusionStats = TRUE,
                                           incremental = TRUE,
                                           incrementalFolder = file.path(outputFolder, "RecordKeeping"),
                                           inclusionStatisticsFolder = outputFolder)
  expect_equal(length(cohortsGenerated), nrow(cohorts))
  unlink(outputFolder)
})

# test_that("Create cohorts - Gen Stats = F, Incremental = F", {
#   outputFolder <- tempdir()
#   # Run first to ensure that all cohorts are generated
#   cohortsGenerated <- instantiateCohortSet(connectionDetails = connectionDetails,
#                                            cdmDatabaseSchema = "main",
#                                            cohortDatabaseSchema = "main",
#                                            cohortTable = "temp_cohort",
#                                            cohorts = cohorts,
#                                            createCohortTable = TRUE,
#                                            generateInclusionStats = FALSE,
#                                            incremental = FALSE,
#                                            incrementalFolder = file.path(outputFolder, "RecordKeeping"),
#                                            inclusionStatisticsFolder = outputFolder)
#   expect_equal(length(cohortsGenerated), nrow(cohorts))
#   unlink(outputFolder)
# })


# Remove the Eunomia database:
rm(connectionDetails)
