# Google Review Analysis 2.0 (Review analysis 1.0 is deprecated) 

- This is currently the 2nd version of Review Analysis. 
- Mainly, we want to focus on extraction of reviews and analyze them based on word count,
high quality word count, keywords, etc.
- 2.0 takes Google Review information from scraped raw data from an executable file and then, using 
Spark protocols to analyze reviews 
- Some implementations are not developed yet from 1.0 -- more to come

#### Advantages of using 2.0 versus 1.0
- Robust
- Unlikely to crash
- Easy to debug
- Fairly easy to put new implementations in (modular programming).
- Runs faster than 1.0 
- Guarantee to parse all data without loss of data.
- Lot cleaner (1.0 was spaghetti and meatballs)

#### Disadvantages of using 2.0 versus 1.0
- If Spark crashes, then whole thing stops running (this actually depends on your computer
computational power) -- highly unlikely!! Most to almost all the time, your computer should run Spark normally.
- No scraping website data. That is a completely separate functionality. 
- Spark is slightly harder to manage if you don't know what you're doing.

#### Features:
- Inputs - raw GOOGLE_REVIEWS.csv and REVIEW_AUTHORS.csv

- Outputs - author_info.csv, sentiment_info.csv, product_info.csv results

- New columns in database: 
    - "Keywords", "KWSentence" are managed by _sentence_analysis.py_
    - "KW Sentiment Score" (Keyword Sentiment Score), "Sentiment Score" are managed by _sentiment_analysis.py_
    - "High Quality Word Count", "Low Quality Word Count", "Word Count", are managed by
    _words_analysis.py_
    
-  (Special feature) - Spark allows you to use UDF (User-defined functions) to help you parse 
review data and perform certain functions onto it (without the struggle of remembering your SQL
functionalities)

#### Installing Requirements:
Add configurations from 'requirements.txt'. If you're using an IDE, it will prompt you
to install the packages. Otherwise, simply run:
- _pip install <package_names>_


#### Expected prerequisites:
You should know how to run Apache Spark on Python IDE. Make sure Apache Spark
and Pypsark package (Python) is running properly before executing the program 


#### Before execution:
Make sure the following are existed in the program:
* 'review_analysis' (for storing csv data)


#### Resources:

#### Steps to run program:
1. Put scraped GOOGLE_REVIEWS.csv and REVIEWS_AUTHORS.csv into review_analysis folder
2. Run 'python report_output.py' - that's it. If any error shows up, report it.
3. Analysis csv files will be in the following folders: author_info, product_info, sentiment_info
