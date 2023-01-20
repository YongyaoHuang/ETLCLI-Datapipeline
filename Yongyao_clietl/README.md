Hi！Thank you for your time watching this.

My main code in Yongyao_clietl\src\Central\etlcli\db.py. Other is cli code and other requirement files that package need.

For the etl pipeline, I tried to built a general pipeline in order to be able to adapt to different scenarios.And it has a certain stability after my test. However, due to time constraints, there are still the following restrictions. (The following problems already beyond the scope of the test, just the problem i think about in real work) I didn't change the program to sql for multi-condition filtering, just use sql for single-condition filtering. At the same time, for the problems that may occur in the program, I have used the original error message from the database. But for example, sql statement errors can be further captured and subdivided by the python's try except module for different processing.

For the code I write, I try to break up the functions into separate modules for easy adjustment/testing.It is also easy to assemble and call up in small parts, and increases the readability of the code. 

This just an easy pipeline code. I am still learning how to improve it. If you have any suggestions/advice, welcome to leave a message!
