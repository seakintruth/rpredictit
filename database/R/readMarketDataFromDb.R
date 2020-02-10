pacman::p_load(RSQLite, DBI)
project.dir <- file.path("/cloud","project","predictit")

readClosedMarketDataFromDb <- function(){
  db = DBI::dbConnect(
    RSQLite::SQLite(), 
    dbname=file.path(project.dir,"predictit.sqlite")
  )
  test.existing.closed.result <- RSQLite::dbSendQuery(
    conn=db,
    "SELECT * FROM market_observations WHERE Status LIKE 'Closed'"
  )
  test.existing.closed.data <-RSQLite::dbFetch(test.existing.closed.result)
  dbClearResult(test.existing.closed.result)
  RSQLite::dbDisconnect(db)
  return(test.existing.closed.data)
}

readOpenMarketDataFromDb <- function(){
  db = DBI::dbConnect(
    RSQLite::SQLite(), 
    dbname=file.path(project.dir,"predictit.sqlite")
  )
  test.existing.open.result <- RSQLite::dbSendQuery(
    conn=db,
    "SELECT * FROM market_observations WHERE Status LIKE 'Open'"
  )
  test.existing.open.data <-RSQLite::dbFetch(test.existing.open.result)
  dbClearResult(test.existing.open.result)
  RSQLite::dbDisconnect(db)
  return(test.existing.open.data)
}

readNullIdMarketDataFromDb <- function(){
  db = DBI::dbConnect(
    RSQLite::SQLite(), 
    dbname=file.path(project.dir,"predictit.sqlite")
  )
  null.id.result <- RSQLite::dbSendQuery(
    conn=db,
    "SELECT DISTINCT id FROM market_id_null"
  )
  null.id.data <-RSQLite::dbFetch(null.id.result)
  dbClearResult(null.id.result)
  RSQLite::dbDisconnect(db)
  return(null.id.data)
}
