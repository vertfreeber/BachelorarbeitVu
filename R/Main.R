library("RPostgreSQL")
library("mlr3verse")
library("dplyr")
library("dbplyr")
library("DBI")
library("glue")
library("mlr3viz")
library("tidyverse")
library("janitor")
library("data.table")
library("mlr3extralearners")
library("gbm")
#library("RMarkdown")

#tablename <- dbListTables(con)
#schema_name <- "yourschema"
drv <- dbDriver("PostgreSQL")
db <- 'omop'  
host_db <- "localhost"  
db_port <- '5432'  
db_user <- "postgres"  
db_password <- "1234"

con <- dbConnect(RPostgres::Postgres(), dbname = db, host=host_db, port=db_port, user=db_user, password=db_password)
#dbListTables(con)
#query <- glue_sql(.con = con, "SELECT * FROM cmd.{`tablename`}")

cohort <- tbl(con, in_schema("results", "cohort" )) %>% rename("person_id" = "subject_id")
condition_occurrence <- tbl(con, in_schema("cmd", "condition_occurrence"))
death <- tbl(con, in_schema("cmd", "death"))
observation <- tbl(con, in_schema("cmd", "observation"))
person <- tbl(con, in_schema("cmd", "person"))
drug_exposure <- tbl(con, in_schema("cmd", "drug_exposure"))

#tables <- c(person, condition_occurrence)
features <- c("observation_concept_id", "death_type_concept_id", "person_id", "drug_concept_id") #"drug_concept_id"
df = left_join(cohort, death, by='person_id') %>%
  left_join(condition_occurrence, by='person_id') %>%
  left_join(observation, by='person_id') %>%
  left_join(person, by='person_id') %>%
  left_join(drug_exposure, by='person_id') %>%
  #filter(cohort_definition_id == 2) %>%
  filter(condition_concept_id == 313217 || condition_concept_id == 315286 || condition_concept_id == 4185932) %>%
 

select(all_of(features)) %>% 
slice_sample(n = 20000)


#df %>% group_by(person_id) %>%
#  mutate(code_num = row_number()) %>%
#  spread(code_num, observation_concept_id) %>%
#  mutate(code_num = row_number()) %>%
#  spread(code_num, drug_concept_id) -> df

#dcast(setDT(df), person_id ~ code_num, value.var = c("observation_concept_id", "drug_concept_id")) -> df
#df <- df %>% 
#  gather(variable, value, -(person_id)) %>%
#  unite(temp, person_id, variable) %>%
#  spread(temp, value)

#df <- df %>% group_by(person_id) %>%
#  mutate(code_num = row_number()) %>% 
#  gather(variable, value, -(c(person_id, code_num, death_type_concept_id))) %>% 
#  unite(temp, code_num, variable) %>% spread(temp, value)

df <- df %>% gather(variable, value, -(c(person_id, death_type_concept_id)))  %>% 
  mutate(value2 = value)  %>% unite(temp, variable, value2) %>% 
  distinct(.keep_all = TRUE) %>% 
  spread(temp, value)

df <-clean_names(df)

#group_by(across(all_of(c("person_id", "temp")))) %>%
#Pivot_wider() cannot handle large data yet.

#df <- dbGetQuery(con, "SELECT condition_concept_id, condition_type_concept_id, death_type_concept_id, gender_concept_id, ethnicity_source_concept_id, drug_concept_id, observation_concept_id   
"FROM cmd.condition_occurrence
LEFT JOIN cmd.observation
ON condition_occurrence.person_id = observation.person_id
LEFT JOIN cmd.drug_exposure
ON condition_occurrence.person_id = drug_exposure.person_id
LEFT JOIN cmd.death
ON condition_occurrence.person_id = death.person_id
INNER JOIN cmd.person
ON person.person_id = condition_occurrence.person_id 
WHERE condition_occurrence.person_id in (
	select condition_occurrence.person_id 
	FROM cmd.condition_occurrence 
	WHERE condition_occurrence.condition_concept_id in (313217, 315286)
	group by condition_occurrence.person_id
	having count(distinct condition_occurrence.condition_concept_id) = 2)
	AND condition_occurrence.condition_concept_id in (313217, 315286)"

#df <- dbGetQuery(con, query)
#cols.dont.want <- c("death_datetime", "region") # if you want to remove multiple columns
#data <- data[, ! names(data) %in% cols.dont.want, drop = F]
#df <- df[,-which(sapply(df, class) == "Date")]
#df <- df[]
df <- subset( df, select = -person_id )
df[] <- lapply(df, as.character)
df <- df %>% replace(is.na(.), "0")
df[df != "0"] <- "1"
df <- df %>% mutate(death_type_concept_id = ifelse(death_type_concept_id == "0", "0", "1")) #Alive=0/Dead=1


df[] <- lapply(df, as.integer)
df2 <- df[c("death_type_concept_id", "observation_concept_id_4015724", 
            "observation_concept_id_2614666", "observation_concept_id_4214956", 
            "observation_concept_id_4046550", "drug_concept_id_2213440", 
            "drug_concept_id_40080069", "observation_concept_id_4058431")]
df["death_type_concept_id"] <- lapply(df["death_type_concept_id"] , factor) #for gradient

task_cadaf = mlr3::as_task_classif(df, target="death_type_concept_id", "cadaf")

print(task_cadaf)
task_cadaf$positive = "1"
split = partition(task_cadaf, ratio = 0.75, stratify = TRUE)
learner = lrn("classif.ranger") #, importance = "impurity"
learner$train(task_cadaf, split$train)
learner$model
prediction = learner$predict_newdata(df[split$test,]) 
measure = msr("classif.acc")

measure$score(prediction, task = task_cadaf, learner = learner, train_set = split$train)

mlr_learners$get("classif.gbm")
learner_gradient = lrn("classif.gbm")
learner_gradient$train(task_cadaf, split$train)
learner_gradient$model
prediction_gradient = learner_gradient$predict_newdata(df[split$test,]) 
measure = msr("classif.acc")
measure$score(prediction_gradient, task = task_cadaf, learner = learner_gradient, train_set = split$train)
#LEFT JOIN cmd.observation
#ON condition_occurrence.person_id = observation.person_id
#LEFT JOIN cmd.drug_exposure
#ON condition_occurrence.person_id = drug_exposure.person_id

afs = auto_fselector(
  method = "random_search",
  learner = learner,
  resampling = rsmp("holdout"),
  measure = measure,
  term_evals = 10,
  batch_size = 5
)

afs$train(task_cadaf, row_ids = split$train)
afs$fselect_result
afs_prediction = afs$predict(task_cadaf, row_ids = split$test)

filter = flt("importance", learner = learner)
filter$calculate(task_cadaf)
head(as.data.table(filter), 10)

learner$param_set


