#This script prompts the user for a file directory.
#It then reads in each file in the directory, modifies the data and performs low-level validation,
#then writes it out and moves it to a server.
#It then calls a bulk insert script to load the file into the database while updating any old rows.

###### some names and documentation has been altered to prevent the release of sensitive information #####

library(RODBC)
library(gWidgets2)
library(gWidgets2RGtk2)
library(rapportools)
library(dplyr)
library(stringr)

#con <- ocdb trusted connection string redacted

#tableMaps <- list("Also" = "Redacted")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#                                                                                             # 
########################################  FUNCTIONS   #########################################
#                                                                                             #
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

###################################   File name parsing   #####################################

parse <- function(cfile){
    #PRE: file name meeting format specifications
    #returns list with table abbreviation and date for the file
    #POST: list with table abbreviation and file date
    return(list("table" = paste("", substr(cfile, 9, 11), sep = ""), 
                "date" = paste("20", substr(cfile, 2,3),"-", substr(cfile, 4,5), sep = "" )))
}
#=============================================================================================#
date <- function(cfile){
    #PRE: file name matching format
    #Finds file date from file name
    #POST: '20[two digit year from file]-[two digit month]
    nl <- nchar(cfile)
    return(paste("20", substr(cfile, nl-5, nl-4),"-", 
                 formatC(match(substr(cfile, nl-8, nl-6), toupper(month.abb)), #match returns index of MONTH in premade month vector
                         width = 2, format = "d", flag = "0"), sep = "")) #formatc formats the month as an integer of width 2
}
#=============================================================================================#
parse_name <- function(cfilename){
    #PRE: file name as a string for a  file
    #POST: list with table abbreviation and file date for file
    if(substr(cfilename, 1,1) == "Y"){
        parse(cfilename)
    }else{
        list("table" = paste("", substr(cfilename, 1,3), sep = ""), "date" = date(cfilename))
    }
}

####################################   Field Validation   #####################################

check_fields <- function(c, isize, icol){
    #PRE: a string value c, an integer isize, an integer icol
    #checks that a value c is within isize. 
    #If it fails, stop the insert and inform the user which cell of which column doesn't comply
    #POST:
    if(length(c) >= isize){
        stop(paste('Potential overflow for value ', c, ", in column ", icol, sep = ""))
    }
}
#=============================================================================================#
check_manual <- function(data, cfile){
    #PRE:takes a file name as a string to use as a header for GUI, and a dataset to show the user
    #Makes a quick GUI showing the dataframe data
    #Gives the user the opportunity to accept or reject the transfer
    #POST: stops the code if 'cancel' is clicked
    status = 'wait'
    win <- gwindow(cfile, visible = FALSE)
        dGroup <- ggroup(container = win, horizontal = FALSE)
            gtable(data[1:100,], container = dGroup)#data table
            bGroup <- ggroup(container = dGroup)#buttons
                gbutton(text = "Load", container = bGroup, handler = function(h, ...){#close window and import
                                                                        assign('status', 'valid', pos = -1, inherits = TRUE)#assign status variable to valid
                                                                        dispose(win)})
                gbutton(text = "Cancel", container = bGroup, handler = function(h, ...){#Close window and stop import
                                                                        assign('status', 'stop', envir = sys.frame(which = 2))
                                                                        stop("Did not load data at user request")
                                                                        dispose(win)})
                                                                        
  visible(win) <- TRUE
  count = 0
  while(status == 'wait' & count < 16560){#timeout after ~ 1hr 
      Sys.sleep(.25)
      count = count + 1
  }
}
#=============================================================================================#
validate <- function(data, file){
    #PRE: dataframe being loaded, and the name of the file for the dataset.
    #Allows the user to view the data, then selects the metadata for the table the data will be inserted into 
    #And checks the data doesn't break null or length rules, and matches the number of columns to be read in.
    #If all checks are passed there should be no output. 
    #If a rule is broken, an error is thrown indicating which value in which column has caused the error.
    #POST: stops the transfer if a problem is imminent
    tbl <- parse_name(file)[["table"]]#Store table name
      
    check_manual(data, file)
    MD <- sqlQuery(con, paste("SELECT * 
                                FROM INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_NAME = '", tableMaps[tbl], "'", sep = ""))
    #Check the 'IS_NULLABLE' column. If 'no', ensure this column doesn't contain any blanks
    not_null <- which(MD$IS_NULLABLE == "NO")
    if(any(is.empty(data[,not_null])) & length(not_null) > 0){#ATTENTION 0 and 0.0 will register as empty. If a column can accept 0 and is marked NOT_NULL there will be issues
        #If above fails a solution may be to check is.null, and apply it using an apply function
        stop("NULL passed into NOT_NULL column")
    }
      
    #Number of rows of metadata should = number of columns of data
    if(ncol(data) != nrow(MD)){
        stop(paste("Columns do not match imported data:", ncol(data), "data structure:", nrow(MD)))
    }
    #Check that the number of characters or digits is within those specified in CHARACTER_MAXIMUM_LENGTH or NUMERIC_PRECISION
}

######################################   Data insert   ########################################

#                                       ----prep----

clearTemp <- function(){
    for(i in 1:length(tableMaps)){
        sqlQuery(con, paste("DELETE FROM",  tableMaps[[i]]))
    }
}
#                                       ----bulk----

bulk <- function(file){
    #PRE: The name of a file on the server to BULK INSERT. Must be in parse_name format.
    #bulk inserts file into the appropriate table based on the file name and tableMaps
    #POST: runs a BULK INSERT query on the server
    tbl <- parse_name(file)[["table"]]
    print(paste("Bulk insert", file, "into", tbl, sep = " "))
    sqlQuery(con, paste("BULK INSERT ", tableMaps[[tbl]],
                        " FROM 'filePath", file, 
                        "' WITH (FIELDTERMINATOR = '||')", sep = ""))
  
}

#                                   ----intermediate----

goHorizontal <- function(tbl, year){
    #PRE: a table abbreviation and year
    #POST: a horizontally partitioned table name for the table and year.
    paste("", substr(tableMaps[[tbl]], 4, nchar(tableMaps[[tbl]])), "_", year, sep = "")
}
#=============================================================================================#
size <- function(char, num, decimal){
    #PRE: character limit, nuber width, and decimal width, 
    #POST: a string with brackets enclosing the appropriate numeric arguments for casting to a given type.
    if(! is.empty(char)){#for varchar
        return(paste(' (', char, ')', sep = ""))
    }else if(! is.empty(decimal)){#for decimal
        return(paste(' (', num, ', ', decimal, ')', sep = ""))
    }else{
        return("")
    }
}
#=============================================================================================#
convert <- function(name, type, char, num, dec){
    #PRE: type data for a column
    #POST: a string CAST statement converting data to match the type of the receiving column
    if(type == 'bigint'){#Handle scientific notation
        paste('CAST(', 'CAST(', name, ' as  real)', " as ", type, size(char, num, dec), ")", sep = "")
    }else{
        paste('CAST(', name, " as ", type, size(char, num, dec), ")", sep = "")
    }
}
#=============================================================================================#
getColumns <- function(table, year){
  #PRE: the abbreviation of the table the data will be inserted into
  #POST: a list of columns, cast to appropriate data types for use in INSERT statemtnt
  column_names <- sqlQuery(con, paste("SELECT *
              FROM INFORMATION_SCHEMA.COLUMNS
              WHERE TABLE_NAME = '", goHorizontal(table, year),  "'", sep = ""))
  cnvrt <- ""
  s <- ""
  for(i in 1:nrow(column_names)){
    cnvrt <- paste(cnvrt, convert(name = column_names[i,]$COLUMN_NAME, #gets CAST string each column
                                  type = column_names[i,]$DATA_TYPE, 
                                  char = column_names[i,]$CHARACTER_MAXIMUM_LENGTH, 
                                  num = column_names[i,]$NUMERIC_PRECISION,
                                  dec = column_names[i,]$NUMERIC_SCALE), sep = s)
    s <- ", "
  }
  return(cnvrt)
}
#=============================================================================================#
insert <- function(table, year){
    #PRE: table abbreviation string, year of the table.
    #POST: runs an INSERT statement to insert data from intermediate tables to horizontally partitioned tables.
  Htable <- goHorizontal(table, year)
  SQL = paste("INSERT INTO ", Htable, 
              " SELECT ", getColumns(table, year),
              " FROM ", tableMaps[table], 
              " WHERE ____ LIKE '", year, "%'", sep = "")
  sqlQuery(con, SQL)
}
#=============================================================================================#
dateCombine = function(){ 
    #Excecute SQL to join time to date column for the  temporary train table, to properly import datetime
    sqlQuery(con, "UPDATE tableName
             SET START_datetime = START_date + ' ' + START_time,
             END_EVENT_datetime = END_date + ' ' + END_time
             WHERE LEN(START_date) < 18")#don't join time to a datetime, if it has already been merged
}
#=============================================================================================#
removeOldMonth = function(table, date){
    #DELETES existing month from the table to allow it to be safely updated
    sqlQuery(con, paste("DELETE FROM ", goHorizontal(table, substring(date, 1, 4)), 
                        " WHERE ____ = ", substring(date, 6, 7), sep = ""))
}
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#                                                                                             #
########################################  MAIN  ###############################################
#                                                                                             #
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
folder <- choose.dir()#select folder to iteratively load files from
tempFolder = paste(folder, "temp", sep = "\\")
unlink(tempFolder, recursive = TRUE)
files <- dir(path = folder)#make list of file names for all .csv files in folder
dir.create(file.path(folder, "temp"))
tableYear <- list()
clearTemp()
for(i in 1:length(files)){
    fileInfo = parse_name(files[i])
    data <- read.csv(paste(folder, files[i], sep = "\\"), header = TRUE) %>%
        cbind(FILE_DTE = fileInfo[["date"]], ___ = substr(fileInfo[["date"]], 6, 7)) %>%
        mutate_all(funs(str_replace_all(., "\\|\\|", "|*")))#replaces ||
#'|' is a special or character for regex. It needs to be escaped with '/' for regex, 
#and '\' needs to be escaped as the argument is a string that is parsed before being evaluated as regex. 
    data[is.na(data)] <- 0
    #Check file name to determine what table and database to import to
    validate(data, files[i])#validates data. User view -> validation with metadata from database
    write.table(data, file = paste(tempFolder, files[i], sep = "\\"),row.names = FALSE, col.names = FALSE, sep = "||", quote = FALSE)#writes data to server
    shell(paste("move ", tempFolder, "\\", files[i], " ", "serverPath", files[i], sep = ""), intern = TRUE)
    bulk(files[i])#bulk insert from server to database
    tableYear[[i]] <- c(fileInfo[["table"]], substr(fileInfo[["date"]], 1, 4))
    removeOldMonth(fileInfo[["table"]], fileInfo[["date"]])
}
unlink(tempFolder, recursive = TRUE)
dateCombine() #combine dates 
#fillMonth() #fill month number column in the temporary tables
tableYear <- unique(tableYear) #Prevent repreated insertion
for(i in 1:length(tableYear)){
    insert(tableYear[[i]][1], tableYear[[i]][2])
}
