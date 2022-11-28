library("mlr3verse")
db <- 'omop'  
host_db <- "localhost"  
db_port <- '5432'  
db_user <- "postgres"  
db_password <- "1234"

con <- dbConnect(RPostgres::Postgres(), dbname = db, host=host_db, port=db_port, user=db_user, password=db_password)

CREATE FUNCTION getAllTablesRows() RETURNS TABLE(source text, date text, value numeric, notes text) AS
$$

DECLARE

    cursorTables CURSOR FOR SELECT table_name FROM information_schema.tables WHERE table_schema = 'cmd';

BEGIN

    FOR tableName IN cursorTables LOOP
        RETURN QUERY EXECUTE format('SELECT %L::text AS source, date, value, notes FROM %I', tableName.table_name, tableName.table_name);
    END LOOP;

END;

$$ LANGUAGE 'plpgsql';