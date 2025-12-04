

library(lytrbox)
library(purrr)

renv::load_vars("~/.zprofile")

lio <- lioapi$new()
lio$account <- list(aid = 6232, apikey = "at.1b874d0579f44253552b0d00b921f1a3.0cca7e1008649c2723f4daa35c264de2")

segment.scan <- function (api, filterql, ent.limit = Inf, page.size = 100, verbose = TRUE, 
    fn = NULL, accumulate = is.null(fn), fields = c()) 
{
    params <- list(field = "_created", sortorder = "desc", limit = page.size, 
        segments = filterql)
    ents <- list()
    total <- 0
    ent.count <- 0
    done <- FALSE
    try({
        body <- list()
        while (!done) {
            scan <- api$exec("api/segment/scan", params = params, 
                body = body, max.retry = 10, return.all = TRUE, 
                method = "POST", as.sysadmin = FALSE)
            if (accumulate) {
                if (length(fields) == 0) {
                  ents <- append(ents, scan$data)
                }
                else {
                  new.ents <- scan$data %>% lapply(extract.list, 
                    fields)
                  ents <- append(ents, new.ents)
                }
            }
            if (!is.null(fn)) {
                lapply(ents, fn)
            }
            if (total == 0) {
                total <- scan$total
            }
            total <- total - (page.size - length(scan$data))
            body$start <- scan$`_next`
            ent.count <- ent.count + length(scan$data)
            if (verbose) {
                cat(paste("ents scanned:", ent.count, "/", total, 
                  "\n"))
            }
            if (ent.count >= total || ent.count >= ent.limit) {
                done <- TRUE
            }
        }
    })
    if (accumulate) {
        return(ents)
    }
    invisible(ent.count)
}

ents <- segment.scan(lio, "FILTER url CONTAINS \"compass-2026\" FROM content")
ents <- lio$exec("api/segment/scan", params = list(field = "_created", sortorder = "desc", limit = 100, segments = "FILTER url CONTAINS \"compass-2026\" FROM content"), body = list(), max.retry = 10, return.all = TRUE, method = "POST", as.sysadmin = FALSE)

json(ents$data[[1]])

transform.ent <- function(ent) {
    topics <- c()
    if(length(ent$taxonomy_region) > 0) {
        topics <- c(topics, paste0("Region:", names(ent$taxonomy_region)))
    }
        if(length(ent$taxonomy_topic) > 0) {
        topics <- c(topics, paste0("Topic:", names(ent$taxonomy_topic)))
    }
    result <- as.list(rep(1, length(topics)))
    names(result) <- paste0("topic_", topics)
    result$url = ent$url
    return(result)
}

library(stringr)
result <- lapply(ents$data, function(ent){
    if(!str_detect(ent$url, "article")) {
        return(NULL)
    }
    cat(ent$url, fill = TRUE)
    lio$post.collect("lytics_content_enrich", transform.ent(ent))
})

json(transform.ent(ents$data[[1]]))
