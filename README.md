# addidas-case-study

Open Library is an initiative of the Internet Archive, a 501(c)(3) non-profit, building a digital library of Internet
sites and other cultural artifacts in digital form. In the section Bulk Data Dumps, they provide public feeds
with the library data.
 https://openlibrary.org/developers/dumps
They also provide a shorter versions of the file for developing or exploratory purposes, where the size is
around 140MB of data instead of ~20GB of the original/full file (referring to the “complete dump”).
 https://s3-eu-west-1.amazonaws.com/csparkdata/ol_cdump.json
Starting with the short version of this file, pls. download it to your local laptop:
wget --continue https://s3-eu-west-1.amazonaws.com/csparkdata/ol_cdump.json -O
/tmp/ol_cdump.json

Please use the JSON file to provide the following information.
1. Load the data
2. Make sure your data set is cleaned enough, so we for example don't include in results with empty/null "titles"
and/or "number of pages" is greater than 20 and "publishing year" is after 1950. State your filters clearly.
3. Run the following queries with the preprocessed/cleaned dataset:
1. Select all "Harry Potter" books
2. Get the book with the most pages
3. Find the Top 5 authors with most written books (assuming author in first position in the array, "key" field and each
row is a different book)
4. Find the Top 5 genres with most books
5. Get the avg. number of pages
6. Per publish year, get the number of authors that published at least one book
