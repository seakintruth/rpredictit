pacman::p_load(dplyr)

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
        if ("rstudioapi" %in% installed.packages()){
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
}

if (is.null(scriptFileName())){
  message("WARNING:Unable to find script path automatically")
  project.dir <- file.path("/media","jeremy","250GbUsb","data","r","predictit")
}else{
  project.dir <- dirname(scriptFileName())  
}

closed.results <- readClosedMarketDataFromDb()
open.results <- readOpenMarketDataFromDb()
null.results <- readNullIdMarketDataFromDb()

market.obs <- closed.results %>%
  dplyr::group_by(id,name,shortName,url,image,timeStamp) %>%
  count()

contract.obs <- closed.results %>%
  dplyr::select(
    id,
    contract_id,
    contract_image,
    contract_name,
    contract_shortName,
    lastTradePrice,
    bestBuyYesCost,
    bestBuyNoCost,
    bestSellYesCost,
    bestSellNoCost,
    lastClosePrice
  )

contract.mid.filter <- contract.obs %>%
  dplyr::filter(lastTradePrice < .90) %>%
  dplyr::filter(lastTradePrice > .10)

undecided.contracts <-  market.obs %>% 
  dplyr::filter(id %in% contract.mid.filter$id)

write.csv(undecided.contracts$url,"undecided_contracts.csv")

list.files()

