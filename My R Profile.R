#===========================================================================
# Default Settings
#===========================================================================

# Stop printing in scientific notation
options(scipen=999)

#===========================================================================
# Read .sql file
#===========================================================================

getSQL <- function(filepath){
  con = file(filepath, "r")
  sql.string <- ""
  
  while (TRUE){
    line <- readLines(con, n = 1)
    
    if ( length(line) == 0 ){
      break
    }
    
    line <- gsub("\\t", " ", line)
    
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

queryYB <- function(query) {
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
  
  # Query and show results
  # sql_query_select<-'SELECT * FROM StewartShouhin;'
  sql_query_select<-query
  df1 <- dbGetQuery(con, sql_query_select) # filter down our data set 
  # print(df1)
  
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

load_df_to_yb <- function(df, my_table_name, is_temp=FALSE, overwrite=FALSE, starting_row=1) {
  
  # Get col types and names
  col_types <- unname(sapply(df, class))
  col_names <- colnames(df)
  
  # Create CREATE TABLE SQL
  if (is_temp==TRUE) {
    sql_create <- paste0('CREATE TEMP TABLE ',my_table_name,' (')
  } else {
    sql_create <- paste0('CREATE TABLE ',my_table_name,' (')
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
  if (overwrite==TRUE){
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
