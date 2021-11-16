source = load 'output' as (MovieId:INT,VoteSum:FLOAT,VoteCount:FLOAT);
movies = load 'input/datasource4/movie_titles.csv' USING PigStorage(',') as (MovieId:INT,Year:INT,Title:CHARARRAY);
joined = JOIN source BY MovieId, movies By MovieId;
result = foreach joined GENERATE Title, Year, VoteSum/VoteCount;
dump result;