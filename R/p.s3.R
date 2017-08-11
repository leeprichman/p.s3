


## ---- import_S3_json
#' Import AWS CLI S3 list-object-versions json output to a data table.
#'
#' @param x Character. AWS CLI s3api json output.
#'
#' @export import_S3_json
#'
#' @import data.table
#' @importFrom base64enc base64encode
#' @importFrom digest hmac
#' @importFrom foreach foreach
#' @importFrom jsonlite fromJSON
#' @importFrom lubridate as_datetime now
#' @importFrom magrittr %>% %T>% %$% %<>%
#' @importFrom parallel detectCores mclapply
#' @importFrom stringr str_extract str_replace str_replace_all
#' @importFrom utils URLencode

import_S3_json <- function(x){
if (readLines(x, n = 10) %>% length < 1) {
                       return(NULL)
                       }
S3_df <- try(jsonlite::fromJSON(txt = x), silent = TRUE)
if (!S3_df$DeleteMarkers %>% is.null) {
  DeleteMarkers <- S3_df$DeleteMarkers %>% data.table::as.data.table
  Owner_name_vector <- DeleteMarkers$Owner[,1]
  Owner_id_vector <- DeleteMarkers$Owner[,2]
  DeleteMarkers$Owner <- NULL
  DeleteMarkers$Owner_name <- Owner_name_vector
  DeleteMarkers$Owner_id <- Owner_id_vector
  DeleteMarkers[, DeleteMarker := TRUE]
} else {
  DeleteMarkers <- NULL
}
if (!S3_df$Versions %>% is.null) {
  Versions <- S3_df$Versions %>% data.table::as.data.table
  Owner_name_vector <- Versions$Owner[,1]
  Owner_id_vector <- Versions$Owner[,2]
  Versions$Owner <- NULL
  Versions$Owner_name <- Owner_name_vector
  Versions$Owner_id <- Owner_id_vector
  Versions[, DeleteMarker := FALSE]
} else {
  Versions <- NULL
}

S3_dt <- data.table::rbindlist(list(DeleteMarkers, Versions), fill = TRUE)
S3_dt$bucket <- x %>% basename %>% gsub("aws_s3_objects_", "", .) %>% gsub("_[0-9]+.json", "", .)

return(S3_dt)
}



## ---- index_S3_objects
#' Index S3 objects from a list of S3 buckets.
#'
#' Quickly index all S3 objects in a bucket list into a data table. Requires AWS CLI.
#'
#' @param buckets Character. List of S3 buckets to index.
#' @param prefix Character. Prefix filter.
#'
#' @export index_S3_objects
#'

index_S3_objects <- function(buckets = system("aws s3 ls | grep -Eo '[a-z0-9\\.]{1,99}$'", intern = TRUE), prefix = NULL) {

commands <- sapply(1:length(buckets), function(x){

if (prefix %>% is.null) commands <- paste0("aws --cli-read-timeout 0 --cli-connect-timeout 0 s3api list-object-versions --bucket ", buckets[x], " > ./aws_s3_objects_", buckets[x], "_", x, ".json;")
if (!prefix %>% is.null) commands <- paste0("aws --cli-read-timeout 0 --cli-connect-timeout 0 s3api list-object-versions --prefix ", prefix, " --bucket ", buckets[x], " > ./aws_s3_objects_", buckets[x], "_", x, ".json;")
  return(commands)

})

cores <- commands %>% length
commands %>% data.table::as.data.table %>%
data.table::fwrite(file = "index_S3_objects.txt", col.names = FALSE, sep = "\t")
system(paste0("/usr/local/bin/parallel --delay 0.2 -j ", buckets %>% length, " :::: index_S3_objects.txt"))

file.remove("index_S3_objects.txt")
json_import <- list.files(path = ".", pattern = "aws_s3_objects.*.json$", full.names = TRUE)

S3_l <- foreach::foreach(j = 1:length(json_import), .errorhandling = "pass", .verbose = TRUE) %dopar% {
  S3_dt <- import_S3_json(json_import[j])
  if (S3_dt %>% is.null) return(NULL)
  return(S3_dt)
}

S3_l %<>% rmListObs
if (S3_l %>% length < 1) return(NULL)

S3_dt <- data.table::rbindlist(S3_l, fill = TRUE)
S3_dt %<>% unique
S3_dt[, full_name := paste0(bucket, "/", Key)]

S3_dt[, LastModified := LastModified %>% gsub("\\.[0-9]+Z", "", .) %>%
    as.POSIXct(format = "%Y-%m-%dT%H:%M:%S") %>%
    lubridate::as_datetime(tz = "EST")]
S3_dt[, LastIndexed := lubridate::now(tz = "EST")]

S3_dt[, Key_first_folder := Key %>%
    stringr::str_extract("[^/]+/")]

S3_dt[, Key_second_folder := Key %>%
    stringr::str_extract("(?<=/)[^/]+/")]
S3_dt[, Key_basename := Key %>% basename]
S3_dt[, Key_dirname := Key %>% basename]

S3_dt[, Key_ext := Key_basename %>%
  stringr::str_extract("(?<=\\.)[^\\.]+$") %>%
  stringr::str_replace("SAVR.*", "")]

S3_dt %>% setkey(LastModified)

list.files(path = ".", pattern = "aws_s3_objects.*.json$", recursive = TRUE, full.names = TRUE) %>% file.remove

return(S3_dt)
}



## ---- print_S3_statistics
#' Print statistics about S3 buckets.
#'
#' @param S3_dt Data table output from index_S3_objects.
#' @param delete_markers Logical. Include delete markers?
#' @param log File path for log output.
#'
#' @export print_S3_statistics
#'

print_S3_statistics <- function(S3_dt, log = "./print_S3_statistics.log", delete_markers = TRUE) {

sink(log)

if (!delete_markers) S3_dt <- S3_dt[DeleteMarker == FALSE]

paste0("Bucket number: ", S3_dt$bucket %>% as.factor %>% levels %>% length) %>% print
paste0("Total objects, all versions:", S3_dt %>% nrow) %>% print
paste0("Bucket number: ", S3_dt$bucket %>% as.factor %>% levels %>% length) %>% print
paste0("Total deletion markers: ", S3_dt[DeleteMarker == TRUE] %>% nrow) %>% print
paste0("Total objects, current versions excluding deletion markers: ", S3_dt[IsLatest == TRUE & DeleteMarker == FALSE] %>% nrow) %>% print

print("Total objects by bucket, all:")
S3_dt[, .("Number of objects" = .N), by = bucket][order(-`Number of objects`)] %>% print

print("Total objects by bucket, current versions excluding deletion markers:")
S3_dt[IsLatest == TRUE & DeleteMarker == FALSE, .("Number of objects" = .N), by = bucket][order(-`Number of objects`)] %>% print

print("Total size by bucket, all:")
S3_dt[, .("Size (GB)" = (Size %>% sum(na.rm = TRUE)/1E9) %>% ceiling), by = bucket][order(-`Size (GB)`)] %>% print

print("Total size by bucket, current versions excluding deletion markers:")
S3_dt[IsLatest == TRUE & DeleteMarker == FALSE, .("Size (GB)" = (Size %>% sum(na.rm = TRUE)/1E9) %>% ceiling), by = bucket][order(-`Size (GB)`)] %>% print

print("Largest files:")
S3_dt[IsLatest == TRUE, .("Size (MB)" = Size/1E6 %>% ceiling, Key)][order(-`Size (MB)`)] %>% head(100) %>% print

print("Largest files by extension:")
S3_dt[IsLatest == TRUE & DeleteMarker == FALSE, .("Size (GB)" = (Size %>% sum(na.rm = TRUE)/1E9) %>% ceiling), by = Key_ext][order(-`Size (GB)`)] %>% head(100) %>% print

print("Largest files key dirname:")
S3_dt[IsLatest == TRUE & DeleteMarker == FALSE, .("Size (GB)" = (Size %>% sum(na.rm = TRUE)/1E9) %>% ceiling), by = Key %>% dirname][order(-`Size (GB)`)] %>% head(100) %>% print

print("Largest previous version files:")
S3_dt[IsLatest == FALSE, .("Size (MB)" = Size/1E6 %>% ceiling, Key)][order(-`Size (MB)`)] %>% head(100) %>% print

print("Largest previous version files by extension:")
S3_dt[IsLatest == FALSE & DeleteMarker == FALSE, .("Size (GB)" = (Size %>% sum(na.rm = TRUE)/1E9) %>% ceiling), by = Key_ext][order(-`Size (GB)`)] %>% head(100) %>% print

print("Largest previous version files key dirname:")
S3_dt[IsLatest == FALSE & DeleteMarker == FALSE, .("Size (GB)" = (Size %>% sum(na.rm = TRUE)/1E9) %>% ceiling), by = Key %>% dirname][order(-`Size (GB)`)] %>% head(100) %>% print

print("Files by extension:")
S3_dt[IsLatest == FALSE & DeleteMarker == FALSE, .N, by = Key_ext][order(-N)] %>% head(100) %>% print

print("Files key dirname:")
S3_dt[IsLatest == FALSE & DeleteMarker == FALSE, .N, by = Key %>% dirname][order(-N)] %>% head(100) %>% print

print("Previous files by extension:")
S3_dt[IsLatest == FALSE & DeleteMarker == FALSE, .N, by = Key_ext][order(-N)] %>% head(100) %>% print

print("Previous files key dirname:")
S3_dt[IsLatest == FALSE & DeleteMarker == FALSE, .N, by = Key %>% dirname][order(-N)] %>% head(100) %>% print

print("Most common keys:")
S3_dt[, .("Common keys" = .N), by = Key][order(-`Common keys`)] %>% head(100) %>% print

print("Most common folders:")
dm_dt <- S3_dt[Key %chin% setdiff(S3_dt[DeleteMarker == TRUE]$Key %>% unique, S3_dt[DeleteMarker == FALSE]$Key %>% unique)]
S3_dt[!Key %chin% dm_dt$Key, .N, by = c("Key_first_folder", "Key_second_folder")][order(-N)] %>% head(100)

print("Common extensions")
S3_dt[, .N, by = Key_ext][order(-N)] %>% head(100) %>% print

print("Only delete markers:")
S3_dt[Key %chin% setdiff(S3_dt[DeleteMarker == TRUE]$Key %>% unique, S3_dt[DeleteMarker == FALSE]$Key %>% unique)] %>% nrow

  sink(NULL)

readLines(log) %>% print

}



## ---- delete_S3_object_version
#' Delete S3 object versions.
#'
#' @param dt Data table. Output from index_S3_objects.
#' @param cores Numeric. Cores to parallelize over.
#' @param safe Logical. Restrict deletions to old versions?
#'
#' @export delete_S3_object_version
#'

delete_S3_object_version <- function(dt, cores = parallel::detectCores()*4, safe) {

dt <- dt[!Key %>% is.na & !bucket %>% is.na]

if (safe) dt <- dt[IsLatest == FALSE]

dt %>% nrow %>% print

if (dt %>% nrow > 0){

commands <- parallel::mclapply(1:nrow(dt), function(x){
  paste0("aws --cli-read-timeout 0 --cli-connect-timeout 0 s3api delete-object --bucket ", dt$bucket[x] %>% shQuote, " --key ", dt$Key[x] %>% shQuote, " --version-id ", dt$VersionId[x] %>% shQuote, ";")
}) %>% unlist
}

if (commands %>% length == 0) return(NULL)

commands %>% data.table::as.data.table %>%
    data.table::fwrite(file = "aws_delete_commands.txt", col.names = FALSE, sep = "\t", quote = FALSE)


system(paste0("/usr/local/bin/parallel -m -j ", cores, " :::: aws_delete_commands.txt"))

}



## ---- get_S3_object_version
#' Download S3 object versions.
#'
#' @param dt Data table. Output from index_S3_objects.
#' @param version Logical. Prepend verion to filename?
#' @param cores Numeric. Cores to parallelize over.
#'
#' @export get_S3_object_version
#'

get_S3_object_version <- function(dt, version = TRUE, cores = parallel::detectCores()*4) {


if (version){

commands <- sapply(1:nrow(dt), function(x){
  paste0("aws --cli-read-timeout 0 --cli-connect-timeout 0 s3api get-object --bucket ", dt$bucket[x] %>% shQuote, " --key ", dt$Key[x] %>% shQuote, " --version-id ", dt$VersionId[x] %>% shQuote, " ", dt$LastModified[x] %>% stringr::str_replace_all("[^0-9]", ""), "_", dt$VersionId[x] %>% stringr::str_replace_all("[^0-9]", ""), "_", dt$Key[x] %>% basename %>% stringr::str_replace_all("[^0-9A-Za-z_\\.]", ""), ";")})
  commands %>% data.table::as.data.table %>%
    data.table::fwrite(file = "aws_get_commands.txt", col.names = FALSE, sep = "\t", quote = FALSE)
}

if (!version){

commands <- sapply(1:nrow(dt), function(x){
  paste0("aws --cli-read-timeout 0 --cli-connect-timeout 0 s3api get-object --bucket ", dt$bucket[x] %>% shQuote, " --key ", dt$Key[x] %>% shQuote, " --version-id ", dt$VersionId[x] %>% shQuote, " ", dt$Key[x] %>% basename %>% stringr::str_replace_all("[^0-9A-Za-z_\\.]", ""), ";")})
  commands %>% data.table::as.data.table %>%
    data.table::fwrite(file = "aws_get_commands.txt", col.names = FALSE, sep = "\t", quote = FALSE)

}

system(paste0("/usr/local/bin/parallel -m -j ", cores, " :::: aws_get_commands.txt"))
file.remove("aws_get_commands.txt")
}



## ---- restore_S3_object_version
#' Restore S3 object versions from AWS Glacier.
#'
#' @param dt Data table. Output from index_S3_objects.
#' @param cores Numeric. Cores to parallelize over.
#' @param days Numeric. Days to restore objects for.
#'
#' @export restore_S3_object_version
#'

restore_S3_object_version <- function(dt, cores = parallel::detectCores()*4, days = 7) {

commands <- sapply(1:nrow(dt), function(x){
paste0("aws --cli-read-timeout 0 --cli-connect-timeout 0 s3api restore-object --bucket ", dt$bucket[x] %>% shQuote, " --key ", dt$Key[x] %>% shQuote, " --version-id ", dt$VersionId[x] %>% shQuote, " --restore-request Days=", days, ";")})
commands %>% data.table::as.data.table %>%
    data.table::fwrite(file = "aws_restore_commands.txt", col.names = FALSE, sep = "\t", quote = FALSE)

system(paste0("/usr/local/bin/parallel -m -j ", cores, " :::: aws_restore_commands.txt"))

file.remove("aws_restore_commands.txt")
}



## ---- copy_S3_object_version
#' Copying S3 object versions.
#'
#' @param dt Data table. Output from index_S3_objects.
#' @param cores Numeric. Cores to parallelize over.
#' @param storage.class Characer. Storage class. One of STANDARD, REDUCED_REDUNDANCY, STANDARD_IA.
#' @param canned.acl Character. Canned ACL to apply to the object. One of private, public-read, public-read-write, authenticated-read, aws-exec-read, bucket-owner-read, bucket-owner-full-control.
#' @param copy.prefix Character. Prefix to copy to, begining with bucket name. NULL will copy the object in place.
#'
#' @export copy_S3_object_version
#'

copy_S3_object_version <- function(dt, cores = parallel::detectCores()*4, storage.class = "STANDARD", canned.acl = "private", copy.prefix = NULL) {

if (!copy.prefix %>% is.null){

commands <- sapply(1:nrow(dt), function(x){
paste0("aws --cli-read-timeout 0 --cli-connect-timeout 0 s3api copy-object --copy-source ", dt$full_name[x] %>% shQuote, " --bucket ", copy.prefix %>% stringr::str_extract("^[^/]+") %>% gsub("/", "", .) %>% shQuote, " --acl ", canned.acl %>% shQuote, " --key ", dt$Key[x] %>% stringr::str_extract("[^/]+$") %>% shQuote, " --metadata-directive COPY --storage-class ", storage.class, ";")})
}
if (copy.prefix %>% is.null){

commands <- sapply(1:nrow(dt), function(x){
paste0("aws --cli-read-timeout 0 --cli-connect-timeout 0 s3api copy-object --copy-source ", dt$full_name[x] %>% shQuote, " --bucket ", dt$bucket[x] %>% shQuote, " --acl ", canned.acl, " --key ", dt$Key[x] %>% shQuote, " --metadata-directive ", metadata.directive, " --storage-class ", storage.class, ";") })
}
commands %>% data.table::as.data.table %>%
  data.table::fwrite(file = "aws_copy_commands.txt", col.names = FALSE, sep = "\t", quote = FALSE)
system(paste0("/usr/local/bin/parallel -m -j ", cores, " :::: aws_copy_commands.txt"))
file.remove("aws_copy_commands.txt")
}



## ---- aws_query_string_auth_url
#' Generating URLs for S3 resources with query string request authentication.
#'
#' Generating URLs, optionally shortened, for S3 resources with query string request authentication for the specified time interval.
#'
#' @param dt Data table. Output from index_S3_objects.
#' @param full_name Character. Objects to lookup.
#' @param region AWS region the bucket is located in.
#' @param aws_access_key_id access key ID for an IAM user with access to the S3 bucket.
#' @param aws_secret_access_key secret access key for an IAM user with access to the S3 bucket.
#' @param lifetime_minutes Numeric. Duration in minutes after which the URL will expire.
#' @param lifetime_days Numeric. Duration in days after which the URL will expire.
#' @param bitly Logical. Generated a shortened url using bit.ly?
#'
#' @export aws_query_string_auth_url
#'

aws_query_string_auth_url <- function(dt = NULL, full_name = NULL, region = "us-east-1", aws_access_key_id = data.table::fread("~/publicread.access.id.txt", header = FALSE)$V1, aws_secret_access_key = data.table::fread("~/publicread.secret.key.txt", header = FALSE)$V1, lifetime_minutes = 60, lifetime_days = NULL, bitly = FALSE) {

expiration_time <- as.integer(Sys.time() + lifetime_minutes * 60)

if (!lifetime_days %>% is.null){
expiration_time <- as.integer(Sys.time() + lifetime_days * 3600 * 24)
}
if (dt %>% is.null){
dt <- data.table::data.table(full_name = full_name, LastModified = NA, Size = NA, bucket = full_name %>% stringr::str_extract("^[^/]+"))
}

urls <- foreach::foreach(i = 1:nrow(dt), .errorhandling = "pass", .verbose = FALSE) %do% {
  canonical_string <- paste0("GET", "\n\n\n", expiration_time, "\n/", dt$full_name[i])
  signature <- digest::hmac(enc2utf8(aws_secret_access_key),
                       enc2utf8(canonical_string), "sha1", raw = TRUE)
  signature_url_encoded <- URLencode(base64enc::base64encode(signature),
                                     reserved = TRUE)

  authenticated_url <- paste0("https://s3.amazonaws.com/",
                              dt$full_name[i],
                              "?AWSAccessKeyId=",
                              enc2utf8(aws_access_key_id),
                              "&Expires=",
                              expiration_time,
                              "&Signature=",
                              signature_url_encoded)
if (bitly){
url <- system(paste0("bitly -l zkcjlsm -k $BITLY_KEY -u '", authenticated_url, "'"), intern = TRUE)
} else {
  url <- NA
}
return(data.table::data.table(Key = dt$full_name %>% basename %>% .[i], auth_url = authenticated_url, short_url = url, LastModified = dt$LastModified[i], Size = dt$Size[i], bucket = dt$bucket[i], markdown_link = paste0("* [", dt$Key[i], "](", authenticated_url, ")")))
}
url_dt <- data.table::rbindlist(urls)
data.table::fwrite(url_dt, "aws_urls.csv")
data.table::fwrite(url_dt[, .(markdown_link)], "aws_url.md", sep = "\t", col.names = FALSE)
return(url_dt)
}
