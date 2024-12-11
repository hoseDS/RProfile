#===========================================================================
# Default Settings
#===========================================================================

# Stop printing in scientific notation
options(scipen=999)

#===========================================================================
# Use Python to read common parameters for yellowbrick queries into yaml
#===========================================================================

library(yaml)

# Run python script in subprocess to fetch common parameters and store in yaml
system('python "C:\\Users\\stewapatte\\OneDrive - Catalina Marketing Japan K.K\\ドキュメント\\Code Chunks\\RProfile\\get_common_parameters.py"', intern=TRUE)

# Read yaml
# common_parameters <- yaml.load_file("C:/Users/stewapatte/OneDrive - Catalina Marketing Japan K.K/ドキュメント/Code Chunks/RProfile/common_parameters.yml")

#===========================================================================
# Substitute common parameters for yellowbrick queries in a string
#===========================================================================

substitute_common_parameters <- function(this_sql_line, common_parameters) {
  this_sql_line<-gsub("\\{common.common_criteria\\(\'tp\'\\)\\}", common_parameters$common_criteria_tp, this_sql_line)
  this_sql_line<-gsub("\\{common.common_chain_exclude\\(\'tp\'\\)\\}", common_parameters$common_chain_exclude_tp, this_sql_line)
  this_sql_line<-gsub("\\{common.common_exclude_ord_event_key_tbl.*\\}", common_parameters$common_exclude_ord_event_key_tbl, this_sql_line)
  this_sql_line<-gsub("\\{common.common_event_dist\\(\'dst\' *, *\'pv\'\\)\\}", common_parameters$common_event_dist_dst_pv, this_sql_line)
  this_sql_line<-gsub("\\{common.common_event_dist\\(\'dst\'\\)\\}", common_parameters$common_event_dist_dst, this_sql_line)
  this_sql_line<-gsub("\\{common.common_id_filter\\(\'pos\'\\)\\}", common_parameters$common_id_filter_pos, this_sql_line)
  this_sql_line<-gsub("\\{common.common_id_filter\\(\'dst\'\\)\\}", common_parameters$common_id_filter_dst, this_sql_line)
  this_sql_line<-gsub("\\{common.common_id_filter\\(\'red\'\\)\\}", common_parameters$common_id_filter_red, this_sql_line)
  this_sql_line<-gsub("\\{common.common_event_ctrl\\(\'dst\' *, *\'pv\'\\)\\}", common_parameters$common_event_ctrl_dst_pv, this_sql_line)
  this_sql_line<-gsub("\\{common.common_event_red\\(\'red\' *, *\'pv\'\\)\\}", common_parameters$common_event_red_red_pv, this_sql_line)
  return(this_sql_line)
}

#===========================================================================
# Read .sql file
#===========================================================================

library(yaml)

getSQL <- function(filepath){
  con = file(filepath, "r")
  sql.string <- ""
  common_parameters = yaml.load_file("C:/Users/stewapatte/OneDrive - Catalina Marketing Japan K.K/ドキュメント/Code Chunks/RProfile/common_parameters.yml")
  
  while (TRUE){
    line <- readLines(con, n = 1)
    
    if ( length(line) == 0 ){
      break
    }
    
    line <- gsub("\\t", " ", line)
    
    line <- substitute_common_parameters(line, common_parameters)
    
    if(grepl("--",line) == TRUE){
      line <- paste(sub("--","/*",line),"*/")
    }
    
    sql.string <- paste(sql.string, line)
  }
  
  close(con)
  return(sql.string)
}

#===========================================================================
# Query yellowbrick (get query)
#===========================================================================

library(DBI)
library(RPostgres)
library(ini)

queryYB <- function(sql_query, big=FALSE) {
  # Database info & credentials
  db <- 'py2jpta1'
  init_file_data<-read.ini("C:/Users/stewapatte/Python/common/config.ini", 
                           encoding = getOption('encoding'))
  
  # Create connection
  con <- dbConnect(Postgres(), 
                   dbname = db, 
                   host=init_file_data$YB$host, 
                   port=init_file_data$YB$port, 
                   user=init_file_data$YB$user, 
                   password=init_file_data$YB$pass)  
  
  # Run SQL query on database
  if (big==FALSE) {
    df1 <- dbGetQuery(con, sql_query) # filter down our data set 
    # If query is big, run it in chunks using LIMIT and OFFSET
  } else {
    rows_returned <- 5000000
    n <- 0
    while (rows_returned>=5000000) {
      n <- n + 1
      print(paste0('Starting Iteration ',n,' at ',now()))
      sql_query_limit <- paste0(sql_query,' ORDER BY 1 LIMIT 5000000 OFFSET ',(n-1)*5000000)
      if (n==1) {
        df1 <- dbGetQuery(con, sql_query_limit) # filter down our data set 
        rows_returned <- nrow(df1)
      } else {
        df_new <- dbGetQuery(con, sql_query_limit)
        df1 <- rbind(df1,df_new)
        rows_returned <- nrow(df_new)
      }
    }
  }
  
  # Close connection
  dbDisconnect(con)
  
  return(df1)
}

#===========================================================================
# Load dataframe to yellowbrick function
#===========================================================================

library(DBI)
library(RPostgres)
library(ini)
library(stringr)
library(tidyverse)

load_df_to_yb <- function(df, my_table_name, is_temp=FALSE, drop_existing=FALSE, starting_row=1) {
  
  # Get col types and names
  col_types <- unname(sapply(df, class))
  col_names <- colnames(df)
  
  # Create CREATE TABLE SQL
  if (is_temp==TRUE) {
    sql_create <- paste0('CREATE TEMP TABLE IF NOT EXISTS ',my_table_name,' (')
  } else {
    sql_create <- paste0('CREATE TABLE IF NOT EXISTS ',my_table_name,' (')
  }
  for (i in 1:length(col_types)) {
    if (grepl(col_types[i], c('character'))) {
      col_type_sql <- 'VARCHAR(512)'
    } else if (grepl(col_types[i], c('logical'))) {
      col_type_sql <- 'BOOLEAN'
    } else {
      col_type_sql <- 'REAL'
    }
    sql_create <- paste0(sql_create,col_names[i],' ',col_type_sql,',')
  }
  sql_create <- substr(sql_create, 1, nchar(sql_create)-1)
  sql_create <- paste0(sql_create,');')
  
  
  # Create DROP SQL
  sql_drop <- paste0('DROP TABLE IF EXISTS ', my_table_name)
  
  # Create connection
  db <- 'py2jpta1'
  # Database info & credentials
  init_file_data<-read.ini("C:/Users/stewapatte/Python/common/config.ini", 
                           encoding = getOption('encoding'))
  con <- dbConnect(Postgres(), 
                   dbname = db, 
                   host=init_file_data$YB$host, 
                   port=init_file_data$YB$port, 
                   user=init_file_data$YB$user, 
                   password=init_file_data$YB$pass)  
  
  # Execute DROP query on SQL Server
  if (drop_existing==TRUE){
    print('Execute DROP')
    dbExecute(con, sql_drop) 
  }
  
  # Execute CREATE query on SQL Server
  print('Execute CREATE')
  dbExecute(con, sql_create) 
  
  # Create INSERT SQL and Execute it
  print('Execute INSERT')
  sql_insert_base <- paste0('INSERT INTO ',my_table_name,' (',paste(col_names,collapse = ', '),') VALUES ')
  for (i in starting_row:nrow(df)) {
    sql_insert <-paste0(sql_insert_base,"('")
    for (j in 1:length(col_names)) {
      sql_insert <- paste0(sql_insert,df[i,j],"', '")
    }
    sql_insert <- substr(sql_insert, 1, nchar(sql_insert)-3)
    sql_insert <- str_replace_all(sql_insert,"'NA'","NULL")
    sql_insert <-paste0(sql_insert,")")
    
    # Execute INSERT query on SQL Server
    result <- tryCatch({
      dbExecute(con, sql_insert)
    }, error = function(err) {
      print(err)
      print(paste0('Inserted rows successfully through: ',i-1,'th row. ',i,'th row failed with SQL shown below'))
      print(sql_insert)
      stop()
    })
  }
  
  # Close connection
  dbDisconnect(con)
}
