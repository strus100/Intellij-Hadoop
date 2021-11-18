source = LOAD 'output_mr3' AS (MovieId:INT,VoteSum:FLOAT,VoteCount:FLOAT);
movies = LOAD 'input/datasource4/movie_titles.csv' USING PigStorage(',') AS (MovieId:INT,Year:INT,Title:CHARARRAY);
joined = JOIN source BY MovieId, movies By MovieId;
secondFiltered = FILTER joined by VoteCount > 1000f;
calculated = FOREACH secondFiltered GENERATE Title, Year, VoteSum/VoteCount AS avg;
grouped = GROUP calculated BY Year;
top = FOREACH grouped {
    sorted = ORDER calculated BY avg DESC;
    topp = LIMIT sorted 3;
    GENERATE FLATTEN(topp);
};
STORE top INTO 'output6';
