pacman::p_load(rpredictit, DBI, RSQLite, png)

scriptFileName <- function() {
  # https://stackoverflow.com/a/32016824/2292993
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

# If we do decied to normalize the tables in the RSQLite database we will need:
# should build out a normalized schema, could use: https://dbdiagram.io/d
# pacman::p_load(uuid)

# A usefull sqlite tutorial:
# https://www.sqlitetutorial.net

# to use the seakintruth fork
# pacman::p_load(devtoools)
# devtools::install_github('seakintruth/rpredictit')

delaySeconds <- 7 
# the api documentation requests calls be a minute apart, but from testing the slow down error message 
# occus after the 10th call in a minute as of 02/11/2019 

marketObservationColumns <- (
  "
    timeStamp DATETIME,
    id INTEGER,
    name TEXT,
    shortName TEXT,
    image TEXT,
    url TEXT,
    status TEXT,
    contract_id INTEGER,
    dateEnd DATETIME,
    contract_image TEXT,
    contract_name TEXT,
    contract_shortName TEXT,
    contract_status TEXT,
    lastTradePrice DOUBLE,
    bestBuyYesCost DOUBLE,
    bestBuyNoCost DOUBLE,
    bestSellYesCost DOUBLE,
    bestSellNoCost DOUBLE,
    lastClosePrice DOUBLE,
    displayOrder INTEGER
    "
)

wait.for.site.maintanence <- function(http.response, check.url){
  # handle a site maintenance message:
  trading.suspended <- http.response$headers$`pi-tradingsuspendedmessage`
  repeat{
    if (!is.null(trading.suspended) ){
      message("API Warning: Trying again in a minute. ",trading.suspended)
      # we just made a call so wait a minute
      query.time.begin <- Sys.time()
      http.response <- httr::GET(check.url)
      # handle a site maintenance message:
      trading.suspended <- http.response$headers$`pi-tradingsuspendedmessage`
      query.time.end <- Sys.time()
      sleep.a.minute(query.time.end, query.time.begin)
    } else {
      break
    }  
  }
}

site.returned.nothing<- function(http.response){
  is.null(
    jsonlite::fromJSON(
      rawToChar(
        http.response$content
      )
    )
  )
}

sleep.a.minute <-function(time.begin = NULL, time.end = NULL){
  if (is.null(time.begin)|is.null(time.end)){
    Sys.sleep(delaySeconds)
  } else {
    if(abs(as.double(difftime(time.begin, time.end, tz, units ="secs")))<delaySeconds){
      Sys.sleep(delaySeconds-abs(as.double(difftime(time.begin, time.end, tz, units ="secs"))))
    }
  }
}

get.closed.market.info <- function(closed.markets,db){
  # rather than using a lapply here a for loop is used in this process as we NEVER want to parallize
  for (market.id in seq_along(closed.markets)){
    query.time.begin <- Sys.time()
    message("getting market: ",closed.markets[market.id])
    get.url <- paste0(
      "https://www.predictit.org/api/marketdata/markets/",
      closed.markets[market.id]
    )
    http.response <- httr::GET(get.url)
    wait.for.site.maintanence(http.response, get.url)
    if(site.returned.nothing(http.response)){
      message("Warning: market ID ",closed.markets[market.id]," returned no content")
      error.checking <- try(
        results <- RSQLite::dbSendQuery(
          conn=db,
          paste0(
            "INSERT INTO market_id_null (id) ",
            "VALUES (", closed.markets[market.id],")"
          )
        )
      )
      dbClearResult(results)
    } else{
      this.market <- rpredictit::single_market(closed.markets[market.id])
      insert.fields <- colnames(this.market)
      message("attempting to query id:",closed.markets[market.id])
      error.checking <- try(
        result <- DBI::dbAppendTable(conn=db,"market_observations",this.market)
      )
    }
    query.time.end <- Sys.time()
    # we just made a call so wait a minute
    sleep.a.minute(query.time.end, query.time.begin)
  }
  DBI::dbDisconnect(db)
}

get.images <- function(db){
  
}

get.closed.markets <- function(){
  # all markets are updated every delaySeconds seconds...
  # so for a history we would need to capture a new timestamp's worth of data every minute...
  # If I was to do this then I need to normalize the data into a table RSQLlight?
  # may need for streaming annalysis?
  # pacman::p_load(stream)
  db = DBI::dbConnect(
    RSQLite::SQLite(), 
    dbname=file.path(project.dir,"predictit.sqlite")
  )
  existing.tables <- DBI::dbListTables(db)
  all.market.data.now <- rpredictit::all_markets()
  #get all closed markets:
  closed.markets <- setdiff(seq(1,max(all.market.data.now$id)),all.market.data.now$id)
  if(length(existing.tables)==0 ){
    # Create the base tables
    # SQL documentation: https://www.sqlite.org/lang.html
    results <- RSQLite::dbSendQuery(
      conn=db,
      paste0(
        "CREATE TABLE market_observations (", 
        marketObservationColumns,",",
        "PRIMARY KEY (timeStamp, id, contract_id)
        )"
      )
    )
    RSQLite::dbClearResult(results)
    results <- RSQLite::dbSendQuery(conn=db,
                                    paste0(
                                      "CREATE TABLE image ( 
                                        imageUrl TEXT,
                                        image BLOB,
                                        PRIMARY KEY (imageUrl)
                                      )"
                                    )
    )
    RSQLite::dbClearResult(results)
    results <- RSQLite::dbSendQuery(
      conn=db,
      paste0(
        "CREATE TABLE market_id_null ( 
          id INTEGER,
          PRIMARY KEY (id)
        )"
      )
    )
    RSQLite::dbClearResult(results)
  } else {
    existing.closed.result <- RSQLite::dbSendQuery(
      conn=db,
      "SELECT DISTINCT id FROM market_observations WHERE Status LIKE 'Closed'"
    )
    existing.closed.data <-RSQLite::dbFetch(existing.closed.result)
    dbClearResult(existing.closed.result)
    null.id.result <- RSQLite::dbSendQuery(
      conn=db,
      "SELECT DISTINCT id FROM market_id_null"
    )
    null.id.data <-RSQLite::dbFetch(null.id.result)
    dbClearResult(null.id.result)
    # get all closed markets that we don't yet have and that we tried, but returned NULL contents
    closed.markets <- base::setdiff(closed.markets,union(unlist(existing.closed.data),unlist(null.id.data)))
  }
  get.closed.market.info(closed.markets, db)
  get.images(db)
  RSQLite::dbDisconnect(db)
}

# Kicks off getting all closed markets daily
repeat{
  query.time.begin.daily <- Sys.time()
  # Repeat forever, getting the new content
  get.closed.markets()
  query.time.end.daily <- Sys.time()
  
  # Delay one day between each call
  # Note: Should probably schedule this script to run daily with the user's OS scheduling solution, 
  # but currently the first run could take roughtly 4 days to finish
  days.worth.of.seconds <- 86400
  if(as.double(difftime(query.time.end.daily,query.time.begin.daily, tz,units ="secs"))<days.worth.of.seconds){
    Sys.sleep(days.worth.of.seconds-as.double(difftime( query.time.end.daily,query.time.begin.daily, tz,units ="secs")))
  }
}
