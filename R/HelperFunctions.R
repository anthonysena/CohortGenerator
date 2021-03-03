#' Load, render, and translate a SQL file in this package.
#' This is done in place of using SqlRender::loadRenderTranslateSql
#' otherwise unit tests will not function properly. 
#' 
#' NOTE: This function does not support dialect-specific SQL translation
#' at this time.
loadRenderTranslateSql <- function(sqlFilename,
                                   dbms = "sql server",
                                   ...,
                                   tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                   warnOnMissingParameters = TRUE) {
  pathToSql <- system.file(paste("sql/sql_server"),
                           sqlFilename,
                           package = "CohortGenerator",
                           mustWork = TRUE)
  sql <- SqlRender::readSql(pathToSql)
  renderedSql <- SqlRender::render(sql = sql,
                                   warnOnMissingParameters = warnOnMissingParameters,
                                   ...)
  renderedSql <- SqlRender::translate(sql = renderedSql,
                                      targetDialect = dbms,
                                      tempEmulationSchema = tempEmulationSchema)
  return(renderedSql)
}