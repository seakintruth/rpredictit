pacman::p_load(RSQLite, DBI)

scriptFileName <- function() {
  # http://stackoverflow.com/a/32016824/2292993
  cmdArgs = commandArgs(trailingOnly = FALSE)
  needle = "--file="
  match = grep(needle, cmdArgs)
  if (length(match) > 0) {
    # Rscript via command line
    return(normalizePath(sub(needle, "", cmdArgs[match])))
  } else {
    ls_vars = ls(sys.frames()[[1]])
    if ("fileName" %in% ls_vars) {
      # Source'd via RStudio
      return(normalizePath(sys.frames()[[1]]$fileName))
    } else {
      if (!is.null(sys.frames()[[1]]$ofile)) {
        # Source'd via R console
        return(normalizePath(sys.frames()[[1]]$ofile))
      } else {
        # RStudio Run Selection
        # http://stackoverflow.com/a/35842176/2292993
        pth = rstudioapi::getActiveDocumentContext()$path
        if (pth!='') {
          return(normalizePath(pth))
        } else {
          # RStudio Console
          tryCatch({
            pth = rstudioapi::getSourceEditorContext()$path
            pth = normalizePath(pth)
          }, error = function(e) {
            # normalizePath('') issues warning/error
            pth = ''
          }
          )
          return(pth)
        }
      }
    }
  }
}

if (nchar(scriptFileName())==0){
  message("WARNING:Unable to find script path automatically")
  project.dir <- file.path("/cloud","project","predictit")
}else{
  project.dir <- dirname(scriptFileName())  
}

.open.predictit.db <- function(project.dir=dirname(scriptFileName())){
  db <- DBI::dbConnect(
    RSQLite::SQLite(), 
    dbname=file.path(project.dir,"predictit.sqlite")
  )
  return(db)
}

readClosedMarketDataFromDb <- function(){
  db <- .open.predictit.db()
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
  db <- .open.predictit.db()
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
  db <- .open.predictit.db()
  null.id.result <- RSQLite::dbSendQuery(
    conn=db,
    "SELECT DISTINCT id FROM market_id_null"
  )
  null.id.data <-RSQLite::dbFetch(null.id.result)
  dbClearResult(null.id.result)
  RSQLite::dbDisconnect(db)
  return(null.id.result)
}
