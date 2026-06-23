**Do not modify code outside of src/ and use all convenctions established in the project**
**You can test and run code with go run main.go**

## Objective ##

You are trying to create a backtester that can run backtests on any number of portfolios, any number of strategies for each portfolio, and any number of assets for each portfolio. The backtester is lean, efficient, and can be built upon easily.

# Tasks to be done

* [x] Fix code in src/ to work with multiple portfolios concurrently  
* [x] Fix strategies to work with new style of running the backtester
* [x] Clean code and iterate by running the program to fix any errors found
* [] Take portfolio and strategy inputs from user. You will read from a TOML file, where the use can specify each portfolio and it's strategy. Test this with a sample input
* []  Test to see if output is producing correctly. If not fix and adjust as needed.
* [] Format the code properly and make sure there aren't any lines over 80 characters
* [] We should only loop through days of each portfolio once in the outer loop. Ensure that we are looping through each day only once for efficiency and fix strategies as needed to adjust to this new style
