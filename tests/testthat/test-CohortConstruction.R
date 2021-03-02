library("testthat")

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

# This test is causing issues whereby the CohortGenerator sql/sql_server
# folder is not found during the test?
# test_that("Create cohorts incrementally", {
#   outputFolder <- tempdir()
#   # Run first to ensure that all cohorts are generated
#   cohortsGenerated <- CohortGenerator::instantiateCohortSet(connectionDetails = connectionDetails,
#                                                             cdmDatabaseSchema = "main",
#                                                             cohortDatabaseSchema = "main",
#                                                             cohortTable = "temp_cohort",
#                                                             cohorts = cohorts,
#                                                             createCohortTable = TRUE,
#                                                             generateInclusionStats = TRUE,
#                                                             incremental = FALSE,
#                                                             incrementalFolder = file.path(outputFolder, "RecordKeeping"),
#                                                             inclusionStatisticsFolder = outputFolder)
#   expect_equal(length(cohortsGenerated), nrow(cohorts))
#   # Next run using incremental mode to verify that all cohorts are created
#   # but the return indicates that nothing new was generated
#   cohortsGenerated <- CohortGenerator::instantiateCohortSet(connectionDetails = connectionDetails,
#                                                             cdmDatabaseSchema = "main",
#                                                             cohortDatabaseSchema = "main",
#                                                             cohortTable = "temp_cohort",
#                                                             cohorts = cohorts,
#                                                             createCohortTable = TRUE,
#                                                             generateInclusionStats = TRUE,
#                                                             incremental = TRUE,
#                                                             incrementalFolder = file.path(outputFolder, "RecordKeeping"),
#                                                             inclusionStatisticsFolder = outputFolder)
#   expect_equal(length(cohortsGenerated), 0)
#   unlink(outputFolder, recursive = TRUE)
# })

# Remove the Eunomia database:
unlink(connectionDetails$server)