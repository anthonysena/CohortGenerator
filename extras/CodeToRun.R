# Get the cohorts from the package
cohortJsonFiles <- list.files(path = system.file("cohorts", package = "CohortGenerator"), full.names = TRUE)
cohorts <- setNames(data.frame(matrix(ncol = 4, nrow = 0), stringsAsFactors = FALSE), c("cohortId","cohortFullName", "sql", "json"))
#cohorts <- data.frame(cohortId = numeric(), cohortFullName = character(), sql = character(), json = character())
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
#View(cohorts)
cohorts$foo <- "a"
connectionDetails <- Eunomia::getEunomiaConnectionDetails()
CohortGenerator::instantiateCohortSet(connectionDetails = connectionDetails,
                                      cdmDatabaseSchema = "main",
                                      cohortDatabaseSchema = "main",
                                      cohortTable = "temp_cohort",
                                      cohorts = cohorts,
                                      createCohortTable = TRUE,
                                      generateInclusionStats = TRUE,
                                      incremental = TRUE,
                                      incrementalFolder = "E:/TEMP/CohortGenerator/Run/RecordKeeping",
                                      inclusionStatisticsFolder = "E:/TEMP/CohortGenerator/Run")
